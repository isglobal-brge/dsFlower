.flower_description_method_table <- function(field) {
  description <- packageDescription("dsFlower")
  methods <- trimws(strsplit(description[[field]], ",", fixed = TRUE)[[1]])
  data.frame(
    name = methods,
    value = paste0("dsFlower::", methods),
    package = "dsFlower",
    version = as.character(packageVersion("dsFlower")),
    type = if (identical(field, "AggregateMethods")) "aggregate" else "assign",
    class = "function",
    stringsAsFactors = FALSE
  )
}

.flower_dslite_config <- function() {
  config <- DSLite::defaultDSConfiguration()
  config$AggregateMethods <- .flower_description_method_table(
    "AggregateMethods")
  config$AssignMethods <- .flower_description_method_table("AssignMethods")
  config
}

test_that("every registered dsFlower method validates lazy arguments first", {
  description <- packageDescription("dsFlower")
  methods <- unique(unlist(lapply(c("AggregateMethods", "AssignMethods"),
    function(field) trimws(strsplit(
      description[[field]], ",", fixed = TRUE)[[1]]))))

  for (method in methods) {
    fn <- get(method, envir = asNamespace("dsFlower"), inherits = FALSE)
    expect_identical(body(fn)[[2L]],
                     quote(.dsflower_require_literal_arguments()),
                     info = method)
    if (length(formals(fn)) == 0L) next

    marker <- new.env(parent = emptyenv())
    marker$ran <- FALSE
    nested <- substitute({
      MARKER$ran <- TRUE
      NULL
    }, list(MARKER = marker))
    expression <- as.call(list(as.name(method), nested))
    expect_error(eval(expression, envir = asNamespace("dsFlower")),
                 "literal values or assigned server symbols", info = method)
    expect_false(marker$ran, info = method)
  }
})

test_that("registered dsFlower outer calls do not initialize nested handles", {
  skip_if_not_installed("DSLite")
  skip_if_not_installed("DSI")

  state_dir <- tempfile("dsflower-expression-state-")
  dir.create(state_dir, recursive = TRUE)
  withr::defer(unlink(state_dir, recursive = TRUE))
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ))

  config <- .flower_dslite_config()
  server <- DSLite::newDSLiteServer(config = config, strict = TRUE,
    tables = list(T = data.frame(x = seq_len(6L), y = rep(0:1, 3L))))
  server_name <- paste0("dsflower_expression_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site",
                               url = server_name)
  withr::defer(DSI::dsDisconnect(connection))
  invisible(DSI::dsAssignTable(connection, "D", "T"))

  before <- length(ls(envir = dsFlower:::.handle_registry, all.names = TRUE))
  expression <- call("flowerStatusDS", call("flowerInitDS", "D"))
  expect_error(
    DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE)),
    "literal values or assigned server symbols")
  after <- length(ls(envir = dsFlower:::.handle_registry, all.names = TRUE))
  expect_identical(after, before)
})
