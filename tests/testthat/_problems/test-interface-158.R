# Extracted from test-interface.R:158

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "dsFlower", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
csv_path <- create_test_csv(n = 50)
on.exit(unlink(csv_path))
handle <- mock_handle(data_path = csv_path, data_format = "csv")
dsFlower:::.setHandle("test_tls", handle)
on.exit(dsFlower:::.removeHandle("test_tls"), add = TRUE)
result <- flowerPrepareRunDS("test_tls", "target", c("f1", "f2", "f3"))
dsFlower:::.setHandle("test_tls", result)
staging_dir <- result$staging_dir
ca_pem <- "-----BEGIN CERTIFICATE-----\nMOCKCERT\n-----END CERTIFICATE-----"
json <- as.character(jsonlite::toJSON(list(pem = ca_pem), auto_unbox = TRUE))
b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
b64 <- gsub("\\+", "-", b64)
b64 <- gsub("/", "_", b64)
b64 <- gsub("=+$", "", b64)
encoded <- paste0("B64:", b64)
local_mocked_bindings(
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL) {
      list(process = NULL, superlink_address = superlink_address,
           ca_cert_path = ca_cert_path)
    }
  )
updated <- flowerEnsureSuperNodeDS("test_tls", "127.0.0.1:9092",
                                      "fl-test", encoded)
