# Test helpers for dsFlower

#' Create a mock Flower handle with new structure
#'
#' @param data_path Character; path to test data file
#' @param data_format Character; data format
#' @param python_path Character; Python executable path
#' @param run_token Character; run token or NULL
#' @param staging_dir Character; staging directory or NULL
#' @param superlink_address Character; SuperLink address or NULL
#' @param target_column Character; target column name
#' @param feature_columns Character vector; feature column names
#' @param prepared Logical; whether run is prepared
#' @param node_ensured Logical; whether SuperNode is ensured
#' @return A list mimicking a Flower handle
mock_handle <- function(data_path = NULL,
                        data_format = "csv",
                        python_path = "python3",
                        run_token = NULL,
                        staging_dir = NULL,
                        superlink_address = NULL,
                        federation_id = NULL,
                        target_column = NULL,
                        feature_columns = NULL,
                        prepared = FALSE,
                        node_ensured = FALSE) {
  list(
    resource_client    = NULL,
    data_path          = data_path,
    data_format        = data_format,
    python_path        = python_path,
    run_token          = run_token,
    staging_dir        = staging_dir,
    superlink_address  = superlink_address,
    federation_id      = federation_id,
    target_column      = target_column,
    feature_columns    = feature_columns,
    prepared           = prepared,
    node_ensured       = node_ensured
  )
}

#' Create a temporary CSV test data file
#'
#' @param n Integer; number of rows
#' @param dir Character; directory to write to (default tempdir)
#' @return Character; path to the CSV file
create_test_csv <- function(n = 50, dir = tempdir()) {
  set.seed(42)
  data <- data.frame(
    f1 = rnorm(n),
    f2 = rnorm(n),
    f3 = rnorm(n),
    f4 = rnorm(n),
    f5 = rnorm(n),
    target = sample(0:1, n, replace = TRUE)
  )
  path <- file.path(dir, "test_train.csv")
  utils::write.csv(data, path, row.names = FALSE)
  path
}
