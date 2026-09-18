"""Survival mathematical, subject-gradient, accounting and authority gates.

Run: python3 dsFlower/inst/python/tests/test_survival_contracts.py
All inputs below are public synthetic fixtures.
"""
import base64
import copy
import json
import math
import os
import sys
import unittest
from unittest import mock

import numpy as np
from scipy import stats
import torch
from opacus import GradSampleModule

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               "..", "..", "flower_app")))
from dsflower_runner import dp_harness, survival


def config(distribution="weibull", edges=None, **changes):
    value = dict(schema_version=1, time_unit="days", time_origin="baseline",
                 t_min=1., horizon=20.)
    if edges is None:
        value.update(time_scale=5., distribution=distribution, dispersion=1.)
    else:
        value.update(edges=edges, horizon=edges[-1])
    value.update(changes)
    return value


def wire(value):
    return {"survival-config-b64": base64.b64encode(json.dumps(value).encode()).decode()}


def criterion(value):
    name = ("discrete_hazard_nll" if "edges" in value
            else "aft_"+value["distribution"]+"_nll")
    return survival.loss_factory(name, wire(value))


def model(width):
    torch.manual_seed(103)
    return torch.nn.Sequential(torch.nn.Linear(2, 4), torch.nn.Tanh(),
                               torch.nn.Linear(4, width)).double()


class SurvivalLossTests(unittest.TestCase):
    def test_aft_independent_density_and_censor_references(self):
        for distribution in ("weibull", "lognormal"):
            for dispersion in survival.DISPERSION_GRID:
                cfg = config(distribution, dispersion=dispersion)
                mu = np.array([-1., .2, 1., 2.])
                time = np.array([1., 3., 10., 20.])
                event = np.array([1., 0., 1., 0.])
                reference = (stats.weibull_min(c=dispersion, scale=5*np.exp(mu))
                             if distribution == "weibull" else
                             stats.lognorm(s=dispersion, scale=5*np.exp(mu)))
                expected = -(event*reference.logpdf(time)
                             +(1-event)*reference.logsf(time)).mean()
                target = torch.tensor(np.column_stack((time,event,np.ones(4))))
                actual = criterion(cfg)(torch.tensor(mu[:,None]), target)
                self.assertAlmostEqual(actual.item(), expected, places=11)

    def test_invalid_remains_in_batch_denominator(self):
        for distribution in ("weibull", "lognormal"):
            loss = criterion(config(distribution))
            pred = torch.tensor([[.2],[.2]], dtype=torch.float64, requires_grad=True)
            target = torch.tensor([[3.,1.,1.],[1.,0.,0.]], dtype=torch.float64)
            self.assertEqual(loss(pred,target).item(), loss(pred[:1],target[:1]).item()/2)
            loss(pred,target).backward()
            self.assertEqual(pred.grad[1].item(), 0.)

    def test_tails_and_bounded_location_derivatives(self):
        for distribution in ("weibull", "lognormal"):
            for dispersion in survival.DISPERSION_GRID:
                cfg = config(distribution, time_scale=1., horizon=1e6,
                             dispersion=dispersion)
                pred = torch.tensor([[-20.],[-10.],[0.],[10.],[20.]],
                                    dtype=torch.float64, requires_grad=True)
                target = torch.tensor([[1e6,0.,1.]]*5, dtype=torch.float64)
                loss = criterion(cfg)(pred,target)
                loss.backward()
                self.assertTrue(torch.isfinite(loss))
                self.assertTrue(torch.isfinite(pred.grad).all())
                self.assertEqual(pred.grad[0].item(),0.)
                self.assertEqual(pred.grad[4].item(),0.)

    def test_interval_end_convention(self):
        cfg = config(edges=[0.,5.,10.,20.])
        target = survival.period_targets([5.,6.,5.,6.,1.,20.],
                                           [1.,1.,0.,0.,0.,0.],np.ones(6),cfg)
        expected = [[1,0,0,1,0,0,1],[0,1,0,1,1,0,1],
                    [0,0,0,1,0,0,1],[0,0,0,1,0,0,1],
                    [0,0,0,0,0,0,1],[0,0,0,1,1,1,1]]
        np.testing.assert_array_equal(target,expected)

    def test_hazard_hand_likelihood_and_all_censored(self):
        cfg = config(edges=[0.,5.,10.,20.])
        target = survival.period_targets([5.,6.,20.],[1.,0.,0.],np.ones(3),cfg)
        probabilities = np.array([[.2,.3,.4],[.1,.2,.3],[.4,.5,.6]])
        logits = np.log(probabilities/(1-probabilities))
        expected = (-math.log(.2)-math.log(.9)-math.log(.6*.5*.4))/9
        actual = criterion(cfg)(torch.tensor(logits),torch.tensor(target,dtype=torch.float64))
        self.assertAlmostEqual(actual.item(),expected,places=12)

    def test_all_invalid_and_empty_draws(self):
        for cfg in (config(),config("lognormal"),config(edges=[0.,5.,20.])):
            width = len(cfg["edges"])-1 if "edges" in cfg else 1
            target = (survival.period_targets([1.,1.],[0.,0.],[0.,0.],cfg)
                      if width > 1 else np.array([[1,0,0],[1,0,0]]))
            pred = torch.ones((2,width),dtype=torch.float64,requires_grad=True)
            value = criterion(cfg)(pred,torch.tensor(target))
            value.backward()
            self.assertEqual(value.item(),0.)
            self.assertTrue(torch.equal(pred.grad,torch.zeros_like(pred)))
            empty = torch.empty((0,width),requires_grad=True)
            criterion(cfg)(empty,torch.empty((0,target.shape[1]))).backward()
            self.assertEqual(empty.grad.shape,empty.shape)

    def test_public_domain_and_unknown_fields_rejected(self):
        bad = [config(dispersion=3),config(dispersion=True),config(horizon=float("inf")),
               config(time_scale=1e-6, horizon=1e6,dispersion=2),
               config(edges=[0.,3.,2.]),config(edges=list(range(66))),
               config(schema_version=True),config(extra=1),config(t_min="1")]
        for cfg in bad:
            with self.subTest(cfg=cfg), self.assertRaises(ValueError):
                survival.validate_survival_config(cfg)
        with self.assertRaises(ValueError):
            survival.validate_survival_config(config(),"aft_lognormal_nll")

    def test_local_prediction_semantics(self):
        for distribution in ("weibull", "lognormal"):
            result = survival.survival_predictions([[0.],[1.]],config(distribution),[0.,5.,20.])
            np.testing.assert_array_equal(result["survival"][:,0],1.)
            self.assertTrue(np.all(np.diff(result["survival"],axis=1)<=0))
            np.testing.assert_array_equal(result["risk"],[0.,-1.])
        result = survival.survival_predictions([[0.,0.,0.]],config(edges=[0.,5.,10.,20.]),[0.,4.,5.,20.])
        np.testing.assert_allclose(result["survival"],[[1.,1.,.5,.125]])
        np.testing.assert_allclose(result["risk"],[-10.])
        np.testing.assert_allclose(result["median"],[5.])


class SurvivalGradientTests(unittest.TestCase):
    def _compare_all_parameters(self,cfg):
        width = len(cfg["edges"])-1 if "edges" in cfg else 1
        net = model(width)
        x = torch.tensor([[.2,-.5],[.3,.8],[.7,-.2]],dtype=torch.float64)
        targets = (survival.period_targets([5.,8.,1.],[1,0,0],[1,1,0],cfg)
                   if "edges" in cfg else np.array([[5.,1.,1.],[8.,0.,1.],[1.,0.,0.]]))
        target = torch.tensor(targets,dtype=torch.float64)
        loss = criterion(cfg)
        def samples(xx,yy):
            hooked = GradSampleModule(copy.deepcopy(net),loss_reduction="mean")
            loss(hooked(xx),yy).backward()
            result = {name:p.grad_sample.detach().clone() for name,p in hooked._module.named_parameters()}
            hooked.remove_hooks()
            return result
        actual=samples(x,target)
        for i in range(len(x)):
            independent=copy.deepcopy(net)
            loss(independent(x[i:i+1]),target[i:i+1]).backward()
            for name,p in independent.named_parameters():
                torch.testing.assert_close(actual[name][i],p.grad,rtol=1e-10,atol=1e-10)
                if i==2:
                    torch.testing.assert_close(actual[name][i],torch.zeros_like(p))
        # Every feature/outcome/mask/validity perturbation is local to subject0.
        variants=[]
        xx=x.clone();xx[0,0]+=2;variants.append((xx,target))
        for col in range(target.shape[1]):
            yy=target.clone()
            yy[0,col]=(yy[0,col]+1 if col==0 and width==1 else 1-yy[0,col])
            variants.append((x,yy))
        for xx,yy in variants:
            changed=samples(xx,yy)
            for name in actual:
                torch.testing.assert_close(changed[name][1:],actual[name][1:],rtol=0,atol=0)
        return net,x,target,actual

    def test_all_parameter_aft_gradients(self):
        for distribution in ("weibull","lognormal"):
            for dispersion in survival.DISPERSION_GRID:
                with self.subTest(distribution=distribution,dispersion=dispersion):
                    self._compare_all_parameters(config(distribution,dispersion=dispersion))

    def test_all_parameter_hazard_gradients_and_one_subject_clip(self):
        cfg=config(edges=[0.,5.,10.,20.])
        net,x,target,samples=self._compare_all_parameters(cfg)
        from opacus.optimizers import DPOptimizer
        hooked=GradSampleModule(net,loss_reduction="mean")
        criterion(cfg)(hooked(x),target).backward()
        optimizer=DPOptimizer(torch.optim.SGD(hooked.parameters(),lr=.1),
            noise_multiplier=1.,max_grad_norm=.2,expected_batch_size=3)
        optimizer.clip_and_accumulate()
        norms=torch.sqrt(sum(g.reshape(3,-1).square().sum(1) for g in samples.values()))
        factors=(.2/(norms+1e-6)).clamp(max=1.)
        for name,p in hooked._module.named_parameters():
            expected=(samples[name]*factors.reshape(3,*([1]*(p.ndim)))).sum(0)
            torch.testing.assert_close(p.summed_grad,expected)
        hooked.remove_hooks()

    def test_cox_negative_control(self):
        for name in ("cox","cox_partial","cox_partial_likelihood"):
            with self.assertRaises(ValueError):dp_harness.loss_from_allowlist(name)
        def cox_gradient(peer):
            z=torch.tensor([.3,peer,.8],requires_grad=True)
            (-z[0]+torch.logsumexp(z,0)).backward()
            return z.grad[0].item()
        self.assertNotEqual(cox_gradient(.1),cox_gradient(2.))

    def test_period_flattening_negative_control(self):
        pred=torch.zeros((2,3),requires_grad=True)
        with self.assertRaises(ValueError):
            criterion(config(edges=[0.,5.,10.,20.]))(pred.reshape(-1,1),torch.zeros((6,3)))


class SurvivalAccountingTests(unittest.TestCase):
    def test_K_never_changes_population_or_effective_mechanism(self):
        policies=[]
        for k in (1,4,16,64):
            cfg=config(edges=np.linspace(0,20,k+1).tolist())
            targets=survival.period_targets(np.ones(17),np.zeros(17),np.zeros(17),cfg)
            self.assertEqual(targets.shape,(17,2*k+1))
            policies.append(dp_harness.effective_dpsgd_mechanism(
                epsilon=4.,delta=1e-5,clipping_norm=1.,n_samples=len(targets),
                batch_size=8,local_epochs=2,num_rounds=3))
        self.assertTrue(all(p==policies[0] for p in policies))
        p=policies[0]
        self.assertEqual(p["accounting_population"],17)
        self.assertEqual(p["sample_rate"],1/3)
        self.assertEqual(p["expected_batch_size"],5)
        self.assertEqual(p["total_steps"],18)
        from opacus.accountants import PRVAccountant
        accountant=PRVAccountant()
        accountant.history=[(p["noise_multiplier"],p["sample_rate"],p["total_steps"])]
        epsilon0=accountant.get_epsilon(delta=1e-5/(1+math.exp(2.)))
        self.assertLessEqual(2*epsilon0,4.01)


if __name__ == "__main__":
    unittest.main(verbosity=2)
