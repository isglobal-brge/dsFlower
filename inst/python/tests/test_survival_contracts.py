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
import tempfile
import shutil
from types import SimpleNamespace

import pandas as pd
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

    def test_period_admin_censor_and_invalid_totalization(self):
        cfg=config(edges=[0.,5.,10.,20.])
        actual=survival.period_targets([21.,20.,.5,float("nan")],[1.,1.,1.,0.],np.ones(4),cfg)
        np.testing.assert_array_equal(actual,[[0,0,0,1,1,1,1],[0,0,1,1,1,1,1],
                                              [0,0,0,0,0,0,0],[0,0,0,0,0,0,0]])

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


class SurvivalRunnerTests(unittest.TestCase):
    def setUp(self):
        from dsflower_runner import task
        self.task = task
        workspace=os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","..",".."))
        self.temp=tempfile.TemporaryDirectory(dir=workspace)
        self.cfg=config()
        self.manifest={"task-type":"survival","dp-track":"neural","dp-unit":"patient",
            "patient_column":"id","patient-id-canonicalization":"trim-utf8-v2",
            "data_type":"tabular","data_format":"csv","data_file":"source.csv",
            "target_column":["time","event"],"feature_columns":["x","z"],
            "survival-config":self.cfg,**wire(self.cfg),"loss-name":"aft_weibull_nll",
            "survival_schema":"subject_survival_v1","survival_file":"subjects.csv",
            "survival_shape":[7,2,3],"survival_feature_columns":["x","z"],
            "survival_target_columns":list(survival.TARGET_COLUMNS),
            "n_samples":8,"n_units":7,"num-classes":2,"num-labels":2,"batch-size":3,
            "local-epochs":1,"num-server-rounds":1}
        self.context=SimpleNamespace(node_config={"manifest-dir":self.temp.name},
                                     run_config=wire(self.cfg))
        source=pd.DataFrame({"id":["001","b","c","c","d","e","f","NA"],
            "x":[1,2,3,4,5,6,7,8],"z":[.1,.2,.3,.4,.5,.6,.7,.8],
            "time":[5,21,6,6,.5,20,8,7],"event":[1,1,0,0,1,1,2,0]})
        self.subjects=pd.DataFrame({"id":["001","b","c","d","e","f",task._MISSING_PATIENT_UNIT],
            "x":[1,2,0,0,6,0,0],"z":[.1,.2,0,0,.6,0,0],
            "__survival_time":[5,20,1,1,20,1,1],"__survival_event":[1,0,0,0,1,0,0],
            "__survival_valid":[1,1,0,0,1,0,0]})
        source.to_csv(os.path.join(self.temp.name,"source.csv"),index=False)
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        self.write_manifest()

    def tearDown(self):
        self.temp.cleanup()

    def write_manifest(self):
        with open(os.path.join(self.temp.name,"manifest.json"),"w") as f:
            json.dump(self.manifest,f)

    def hazard_fixture(self):
        self.cfg=config(edges=[0.,5.,10.,20.])
        self.manifest.update({"survival-config":self.cfg,**wire(self.cfg),
            "loss-name":"discrete_hazard_nll","survival_shape":[7,2,9]})
        self.context.run_config=wire(self.cfg)
        d=np.array([[1,0,0],[0,0,0],[0,0,0],[0,0,0],[0,0,1],[0,0,0],[0,0,0]])
        mask=np.array([[1,0,0],[1,1,1],[0,0,0],[0,0,0],[1,1,1],[0,0,0],[0,0,0]])
        for prefix,values in (("d",d),("m",mask)):
            for j in range(3):self.subjects["__survival_%s_%d"%(prefix,j+1)]=values[:,j]
        self.manifest["survival_target_columns"]=list(self.subjects.columns[3:])
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        self.write_manifest()

    def test_hazard_artifact_reconstruction_and_authority(self):
        self.hazard_fixture()
        x,y,ids,m=self.task.load_survival_data(self.context)
        self.assertEqual(m,8);self.assertEqual(y.shape,(7,7))
        np.testing.assert_array_equal(y[0],[1,0,0,1,0,0,1])
        np.testing.assert_array_equal(y[1],[0,0,0,1,1,1,1])
        self.subjects.loc[0,"__survival_m_2"]=1
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        with self.assertRaises(RuntimeError):self.task.load_survival_data(self.context)

    def test_hazard_preserves_adjacent_float_interval_boundary(self):
        self.hazard_fixture()
        value=np.nextafter(5.,np.inf)
        self.subjects["__survival_time"]=self.subjects["__survival_time"].astype(float)
        source=pd.read_csv(os.path.join(self.temp.name,"source.csv"),dtype={"id":str},keep_default_na=False)
        source["time"]=source["time"].astype(float)
        source.loc[0,"time"]=value
        source.to_csv(os.path.join(self.temp.name,"source.csv"),index=False)
        self.subjects.loc[0,["__survival_time","__survival_d_1","__survival_d_2","__survival_m_2"]]=[value,0,1,1]
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        _,y,_,_=self.task.load_survival_data(self.context)
        np.testing.assert_array_equal(y[0],[0,1,0,1,1,0,1])

    def test_hazard_training_dispatch_preserves_N_and_M(self):
        from dsflower_runner import client_app
        self.hazard_fixture()
        pins={**self.task.load_run_pins(self.context),"round_index":1}
        cfg=self.task.load_pinned_run_config(self.context)
        pcfg={"epsilon":4.,"delta":1e-5,"clipping_norm":1.,"n_samples":8}
        with mock.patch.object(client_app,"_pool_by_patient",side_effect=AssertionError("generic pooling")), \
             mock.patch.object(client_app,"_dp_fit",return_value=([],7)) as fit, \
             mock.patch.object(client_app.seeding,"master_seed",return_value=b"a"*32), \
             mock.patch.object(client_app.dp_harness,"effective_dpsgd_mechanism",
                return_value={"noise_multiplier":1.,"policy_hash":"f"*64}) as effective:
            client_app._train_neural(self.context,cfg,pcfg,pins,model(3).float(),2,False)
        self.assertEqual(effective.call_args.kwargs["n_samples"],7)
        args=fit.call_args.args
        self.assertEqual(args[1].shape,(7,2))
        self.assertEqual(args[2].shape,(7,7))
        self.assertEqual(args[5],8)
        self.assertEqual(fit.call_args.kwargs["geometry_n_units"],None)

    def test_survival_private_validation_and_resampling_are_rejected(self):
        from dsflower_runner import validation
        for loss in survival.SURVIVAL_LOSSES:
            # Even a hostile existing-task label cannot sneak in a survival loss.
            cfg={"loss-name":loss,"validation-task":"binary","task-type":"regression",
                 "target-bounds":{"lower":0.,"upper":20.}}
            for loader in (validation.layout_from_config,validation.holdout_layout_from_config,
                           validation.cross_validation_layout_from_config):
                with self.assertRaises(ValueError):loader(cfg)

    def test_hazard_width_sticky_grid_and_initialization(self):
        from dsflower_runner import client_app,model_spec,seeding,server_app,params
        self.hazard_fixture()
        pins=self.task.load_run_pins(self.context)
        cfg=self.task.load_pinned_run_config(self.context)
        self.assertEqual(model_spec.output_width("discrete_hazard_nll",cfg),3)
        self.assertEqual(client_app._prep_target(np.zeros((7,7)),pins["loss_name"],2).shape,(7,7))
        first,_=client_app._neural_seed_contract(cfg,pins,{})
        second,_=client_app._neural_seed_contract(wire(config(edges=[0.,4.,10.,20.])),pins,{})
        self.assertNotEqual(first,second)
        cfg.update({"num-features":2,"model-spec-b64":base64.b64encode(
            json.dumps({"layers":[{"op":"linear","out":"@out"}]}).encode()).decode()})
        torch.manual_seed(13);a=server_app._build_initial_model(cfg)
        torch.manual_seed(71);b=server_app._build_initial_model(cfg)
        self.assertEqual(a(torch.zeros((2,2))).shape,(2,3))
        for aa,bb in zip(params.get_torch_params(a),params.get_torch_params(b)):
            np.testing.assert_array_equal(aa,bb)

    def test_source_and_subject_census_and_invalid_totalization(self):
        x,y,ids,m=self.task.load_survival_data(self.context)
        self.assertEqual(m,8);self.assertEqual(len(y),7)
        np.testing.assert_array_equal(y[:,2],[1,1,0,0,1,0,0])
        np.testing.assert_array_equal(x[y[:,2]==0],np.zeros((4,2)))
        np.testing.assert_array_equal(y[:,0],[5,20,1,1,20,1,1])
        self.assertEqual(ids[0],"001")

    def test_staging_tamper_fails_closed(self):
        for key,value in (("n_samples",7),("n_units",6),("survival_shape",[8,2,3])):
            original=self.manifest[key];self.manifest[key]=value;self.write_manifest()
            with self.assertRaises((RuntimeError,ValueError)):
                self.task.load_survival_data(self.context)
            self.manifest[key]=original
        self.write_manifest()
        self.subjects.loc[0,"__survival_valid"]=0
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        with self.assertRaises(RuntimeError):self.task.load_survival_data(self.context)

    def test_public_preflight_before_private_reads(self):
        for key,value in (("dp-unit","row"),("task-type","classification"),
                          ("target_column",["time","x"]),
                          ("cv-contract-sha256","a"*64)):
            previous=self.manifest.copy();self.manifest[key]=value;self.write_manifest()
            with mock.patch.object(self.task,"_read_staged_frame",side_effect=AssertionError("private read")):
                with self.assertRaises(ValueError):self.task.load_survival_data(self.context)
            self.manifest=previous
        self.write_manifest()

    def test_server_config_cannot_be_overridden(self):
        pinned=self.task.load_pinned_run_config(self.context)
        self.assertEqual(pinned["survival-config"],self.cfg)
        self.context.run_config=wire(config(dispersion=2.))
        with self.assertRaises(ValueError):self.task.load_pinned_run_config(self.context)
        self.context.run_config=wire(self.cfg)
        self.context.run_config["survival-config"]=config("lognormal")
        with self.assertRaises(ValueError):self.task.load_pinned_run_config(self.context)

    def test_irrelevant_class_pins_are_canonical_and_cannot_rekey(self):
        from dsflower_runner import client_app
        pins=self.task.load_run_pins(self.context)
        base,_=client_app._neural_seed_contract(wire(self.cfg),pins,{})
        explicit={**wire(self.cfg),"num-classes":2,"num-labels":2}
        self.assertEqual(base,client_app._neural_seed_contract(explicit,pins,{})[0])
        for key in ("num-classes","num-labels"):
            self.context.run_config={**wire(self.cfg),key:3}
            with self.assertRaises(ValueError):self.task.load_pinned_run_config(self.context)
            self.context.run_config=wire(self.cfg)
            for value in (3,2.,True,None):
                self.manifest[key]=value;self.write_manifest()
                with mock.patch.object(self.task,"_read_staged_frame",side_effect=AssertionError("private read")):
                    with self.assertRaises(ValueError):self.task.load_survival_data(self.context)
            self.manifest[key]=2;self.write_manifest()

    def test_loss_pins_and_release_shapes(self):
        from dsflower_runner import client_app,model_spec
        pins=self.task.load_run_pins(self.context)
        self.assertEqual(pins["loss_name"],"aft_weibull_nll")
        self.assertEqual(model_spec.output_limit_for_loss(pins["loss_name"]),10.)
        y=np.array([[5,1,1],[1,0,0]],dtype=np.float32)
        self.assertEqual(tuple(client_app._prep_target(y,pins["loss_name"],2).shape),(2,3))
        self.assertIsNotNone(dp_harness.loss_from_allowlist(pins["loss_name"],wire(self.cfg)))

    def test_sticky_effective_semantics(self):
        from dsflower_runner import client_app,seeding
        pins=self.task.load_run_pins(self.context)
        x,y,ids,m=self.task.load_survival_data(self.context)
        privacy={"policy_hash":"f"*64}
        def digest(cfg=wire(self.cfg),target=y,features=x,pinned=pins):
            semantic,_=client_app._neural_seed_contract(cfg,pinned,{})
            return seeding._semantic_digest("survival-test",semantic,privacy,1,
                    private_arrays=(features,target),execution_fingerprint={})
        baseline=digest()
        moved=wire(self.cfg);moved["run-token"]="new-token";moved["results-dir"]="new-path"
        self.assertEqual(baseline,digest(moved))
        self.assertNotEqual(baseline,digest(wire(config(dispersion=2.))))
        changed=y.copy();changed[0,2]=0
        self.assertNotEqual(baseline,digest(target=changed))
        changed=x.copy();changed[0,0]+=1
        self.assertNotEqual(baseline,digest(features=changed))
        changed_pins={**pins,"loss_name":"aft_lognormal_nll"}
        self.assertNotEqual(baseline,digest(wire(config("lognormal")),pinned=changed_pins))

    def test_inert_survival_wire_fields_preserve_semantics(self):
        from dsflower_runner import client_app,seeding
        extras=({"model":"unused_public_alias"},{"data-kind":"tabular"},
                {"target-bounds":"unused"})
        for variant in ("weibull","lognormal","hazard"):
            if variant=="hazard":
                self.hazard_fixture()
            else:
                self.cfg=config(variant)
                self.manifest.update({"survival-config":self.cfg,**wire(self.cfg),
                                      "loss-name":"aft_%s_nll"%variant})
                self.write_manifest()
            pins=self.task.load_run_pins(self.context)
            def digest(extra):
                self.context.run_config={**wire(self.cfg),**extra}
                cfg=self.task.load_pinned_run_config(self.context)
                semantic,_=client_app._neural_seed_contract(cfg,pins,{})
                return seeding._semantic_digest("survival-wire-test",semantic,
                    {"policy_hash":"f"*64},1,execution_fingerprint={})
            baseline=digest({})
            for extra in extras:
                with self.subTest(variant=variant,field=next(iter(extra))):
                    self.assertEqual(baseline,digest(extra))
        # Existing contracts retain their prior handling of these public keys.
        for extra in extras:
            semantic,_=client_app._neural_seed_contract(
                {"loss-name":"mse",**extra},{"loss_name":"mse"},{})
            for key,value in extra.items():self.assertEqual(semantic["run"][key],value)

    def test_sticky_repeated_released_arrays_and_effective_changes(self):
        from dsflower_runner import client_app,params
        def execute(changed_incoming=False,operational=False,extra=None):
            pins={**self.task.load_run_pins(self.context),"round_index":1}
            self.context.run_config={**wire(self.cfg),**(extra or {})}
            cfg=self.task.load_pinned_run_config(self.context)
            if operational:cfg.update({"run-token":"different-token","results-dir":"different-path"})
            width=len(self.cfg["edges"])-1 if "edges" in self.cfg else 1
            net=model(width).float()
            net._dsflower_release_keys=tuple(name for name,_ in net.named_parameters())
            if changed_incoming:
                with torch.no_grad():next(net.parameters()).add_(.01)
            pcfg={"epsilon":4.,"delta":1e-5,"clipping_norm":1.,"n_samples":8}
            with mock.patch.object(client_app.seeding,"_node_secret",return_value=b"s"*32):
                result,_=client_app._train_neural(self.context,cfg,pcfg,pins,net,2,False)
            return [a.copy() for a in result]
        def same(a,b):return all(np.array_equal(x,y) for x,y in zip(a,b))
        first=execute()
        self.assertTrue(same(first,execute()))
        self.assertTrue(same(first,execute(operational=True)))
        extras=({"model":"unused_public_alias"},{"data-kind":"tabular"},
                {"target-bounds":"unused"})
        for extra in extras:self.assertTrue(same(first,execute(extra=extra)))
        rebound=os.path.join(self.temp.name,"rebound")
        os.mkdir(rebound)
        for name in ("source.csv","subjects.csv","manifest.json"):
            shutil.copyfile(os.path.join(self.temp.name,name),os.path.join(rebound,name))
        self.context.node_config["manifest-dir"]=rebound
        self.assertTrue(same(first,execute(operational=True)))
        self.context.node_config["manifest-dir"]=self.temp.name
        self.assertFalse(same(first,execute(changed_incoming=True)))
        # A public dispersion/distribution change rekeys actual noise and training.
        self.cfg=config(dispersion=2.)
        self.manifest.update({"survival-config":self.cfg,**wire(self.cfg)})
        self.context.run_config=wire(self.cfg);self.write_manifest()
        self.assertFalse(same(first,execute()))
        self.cfg=config("lognormal")
        self.manifest.update({"survival-config":self.cfg,**wire(self.cfg),"loss-name":"aft_lognormal_nll"})
        self.context.run_config=wire(self.cfg);self.write_manifest()
        self.assertFalse(same(first,execute()))
        lognormal_first=execute()
        for extra in extras:self.assertTrue(same(lognormal_first,execute(extra=extra)))
        # Complete grid vectors also bind retries; both grids have identical K.
        self.hazard_fixture()
        hazard_first=execute()
        for extra in extras:self.assertTrue(same(hazard_first,execute(extra=extra)))
        self.cfg=config(edges=[0.,6.,10.,20.])
        self.manifest.update({"survival-config":self.cfg,**wire(self.cfg)})
        self.context.run_config=wire(self.cfg);self.write_manifest()
        self.assertFalse(same(hazard_first,execute()))
        # A newly invalid subject preserves N, changes effective y and rekeys.
        source=pd.read_csv(os.path.join(self.temp.name,"source.csv"),dtype={"id":str},keep_default_na=False)
        source.loc[0,"event"]=2
        source.to_csv(os.path.join(self.temp.name,"source.csv"),index=False)
        self.subjects.loc[0,self.subjects.columns[1:]]=0.
        self.subjects.loc[0,"__survival_time"]=1.
        self.subjects.to_csv(os.path.join(self.temp.name,"subjects.csv"),index=False)
        self.assertFalse(same(hazard_first,execute()))

    def test_reply_releases_only_parameters_and_fixed_weight(self):
        from dsflower_runner import client_app
        from flwr.common import Message,RecordDict,ArrayRecord
        message=Message(content=RecordDict({"arrays":ArrayRecord(numpy_ndarrays=[np.ones(2,dtype=np.float32)])}),
                        dst_node_id=1,message_type="train")
        reply=client_app._reply(message,[np.zeros(2,dtype=np.float32)])
        self.assertEqual(set(reply.content),{"arrays","metrics"})
        self.assertEqual(dict(reply.content["metrics"]),{"num-examples":1})

    def test_all_invalid_runs_normal_noisy_optimizer(self):
        from dsflower_runner import client_app,params
        pins={**self.task.load_run_pins(self.context),"round_index":1}
        cfg=wire(self.cfg)
        pcfg={"epsilon":4.,"delta":1e-5,"clipping_norm":1.,"n_samples":8}
        x=np.zeros((7,2),dtype=np.float32)
        y=np.tile([1.,0.,0.],(7,1)).astype(np.float32)
        net=model(1).float()
        net._dsflower_release_keys=tuple(name for name,_ in net.named_parameters())
        before=[a.copy() for a in params.get_torch_params(net)]
        result,n=client_app._dp_fit(net,x,y,pcfg,pins,8,cfg,b"a"*32,1.)
        self.assertEqual(n,7)
        self.assertTrue(any(not np.array_equal(a,b) for a,b in zip(result,before)))
        self.assertTrue(all(np.isfinite(a).all() for a in result))


if __name__ == "__main__":
    unittest.main(verbosity=2)
