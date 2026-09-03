test_that("imaging discovery returns only public structural manifest fields", {
  name <- "test_public_image_metadata"
  descriptor <- structure(list(
    dataset_id = "public.dataset",
    source_kind = "image_bundle",
    manifest = list(
      metadata = list(label_col = "diagnosis"),
      labels = list(
        list(
          name = "legacy_private_label", type = "categorical",
          columns = c("legacy_private_label", "diagnosis_group"),
          description = "Legacy schema must be ignored",
          uri = "s3://private/labels.parquet",
          private_values = c("case", "control")
        )
      )
    ),
    assets = list(
      images = list(
        kind = "image_root", provider = "hospital-pacs",
        uri = "s3://private/images", n_samples = 73L
      ),
      radiomics = list(
        kind = "feature_table", processor = "pyradiomics",
        path = "/private/features.parquet", completed_n = 68L
      ),
      tumour_masks = list(
        kind = "mask_root", segmenter = "nnunet",
        root = "/private/masks", n_valid = 65L
      )
    )
  ), class = "FlowerDatasetDescriptor")
  dsFlower:::.setHandle(name, list(
    source = "descriptor", source_kind = "image_bundle",
    descriptor = descriptor, prepared = FALSE, node_ensured = FALSE
  ))
  withr::defer(dsFlower:::.removeHandle(name))

  labels <- flowerImageLabelsDS(name)
  expect_named(labels, c("name", "type", "columns", "description"))
  expect_equal(labels$name, "diagnosis")
  expect_equal(labels$type, "declared_label")
  expect_equal(labels$columns, "diagnosis")
  expect_false("legacy_private_label" %in% labels$name)
  expect_false(any(c("uri", "private_values") %in% names(labels)))

  assets <- flowerImageAssetsDS(name)
  expect_named(assets, c("alias", "kind", "provider"))
  expect_equal(assets$alias, c("images", "radiomics", "tumour_masks"))
  expect_equal(assets$provider, c("hospital-pacs", "pyradiomics", "nnunet"))
  expect_false(any(c("uri", "path", "root", "n_samples", "completed_n") %in%
                   names(assets)))

  masks <- flowerImageMasksDS(name)
  expect_named(masks, c("alias", "provider", "status"))
  expect_equal(masks$alias, "tumour_masks")
  expect_equal(masks$status, "declared")
  expect_false(any(c("n_valid", "root", "path") %in% names(masks)))
})

test_that("imaging discovery never reflects path-like declarative fields", {
  name <- "test_canonical_image_metadata"
  descriptor <- structure(list(
    dataset_id = "public.dataset",
    source_kind = "image_bundle",
    manifest = list(metadata = list(label_col = "diagnosis")),
    assets = list(
      path_provider = list(kind = "feature_table",
        provider = "/srv/private/models/model.pt"),
      uri_processor = list(kind = "feature_table",
        processor = "s3://private-bucket/features"),
      scheme_segmenter = list(kind = "mask_root",
        segmenter = "file:private-mask"),
      bad_kind = list(kind = "private kind", provider = "safe-provider"),
      `s3://private-bucket/asset` = list(
        kind = "image_root", provider = "safe-provider")
    )
  ), class = "FlowerDatasetDescriptor")
  dsFlower:::.setHandle(name, list(
    source = "descriptor", source_kind = "image_bundle",
    descriptor = descriptor, prepared = FALSE, node_ensured = FALSE
  ))
  withr::defer(dsFlower:::.removeHandle(name))

  assets <- flowerImageAssetsDS(name)
  expect_equal(assets$alias,
    c("path_provider", "uri_processor", "scheme_segmenter", "bad_kind"))
  expect_equal(assets$provider,
    c("unknown", "unknown", "unknown", "safe-provider"))
  expect_equal(assets$kind,
    c("feature_table", "feature_table", "mask_root", "unknown"))
  public <- jsonlite::toJSON(assets)
  expect_false(grepl("/srv/private", public, fixed = TRUE))
  expect_false(grepl("s3://", public, fixed = TRUE))
  expect_false(grepl("file:private", public, fixed = TRUE))
  expect_false(grepl("private kind", public, fixed = TRUE))
})

test_that("imaging label discovery accepts only canonical public identifiers", {
  private_values <- c(
    "/srv/private/labels.parquet", "s3://private-bucket/labels",
    "file:private-label", "patient label")

  for (i in seq_along(private_values)) {
    name <- paste0("test_private_label_column_", i)
    descriptor <- structure(list(
      source_kind = "image_bundle",
      manifest = list(metadata = list(label_col = private_values[[i]]))
    ), class = "FlowerDatasetDescriptor")
    dsFlower:::.setHandle(name, list(
      source = "descriptor", source_kind = "image_bundle",
      descriptor = descriptor, prepared = FALSE, node_ensured = FALSE
    ))

    labels <- flowerImageLabelsDS(name)
    dsFlower:::.removeHandle(name)
    expect_equal(nrow(labels), 0L)
    expect_false(grepl(private_values[[i]], jsonlite::toJSON(labels),
                       fixed = TRUE))
  }
})

test_that("imaging discovery is type-stable for non-imaging handles", {
  name <- "test_non_image_metadata"
  dsFlower:::.setHandle(name, list(
    source = "table", table_data = data.frame(x = 1:3),
    prepared = FALSE, node_ensured = FALSE
  ))
  withr::defer(dsFlower:::.removeHandle(name))

  expect_named(flowerImageLabelsDS(name),
               c("name", "type", "columns", "description"))
  expect_named(flowerImageAssetsDS(name), c("alias", "kind", "provider"))
  expect_named(flowerImageMasksDS(name), c("alias", "provider", "status"))
  expect_equal(nrow(flowerImageLabelsDS(name)), 0L)
  expect_equal(nrow(flowerImageAssetsDS(name)), 0L)
  expect_equal(nrow(flowerImageMasksDS(name)), 0L)
})

test_that("public imaging discovery methods are registered aggregates", {
  aggregate <- packageDescription("dsFlower")$AggregateMethods
  registered <- trimws(strsplit(aggregate, ",", fixed = TRUE)[[1]])
  expect_true(all(c(
    "flowerImageLabelsDS", "flowerImageAssetsDS", "flowerImageMasksDS"
  ) %in% registered))
})
