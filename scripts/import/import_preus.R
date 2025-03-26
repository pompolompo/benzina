# Libraries ---------------------------------------------------------------
library(dplyr)
library(purrr)
library(httr2)
library(DBI)

# Options -----------------------------------------------------------------
ccaa <- "09" # catalunya
prod <- "01" # benzina
prev <- as.Date("2023-06-23")
dies <- seq.Date(from = prev, to = Sys.Date() - 2, by = "day") %>%
  format("%d-%m-%Y")


# Database ----------------------------------------------------------------
con <- dbConnect(RSQLite::SQLite(), "dbase/sqlite/benzina.sqlite")

# Download and Save -------------------------------------------------------
temps <- Sys.time()

split(dies, cut(seq_along(dies), breaks = round(length(dies) / 100))) %>%
  walk(\(interval){
    preus <- interval %>%
      map(\(dia){
        paste0(
          "https://sedeaplicaciones.minetur.gob.es",
          "/ServiciosRESTCarburantes/PreciosCarburantes",
          "/EstacionesTerrestresHist/FiltroCCAAProducto",
          "/", dia, "/", ccaa, "/", prod
        ) %>%
          request() %>%
          req_perform() %>%
          resp_body_json() %>%
          .[["ListaEESSPrecio"]] %>%
          bind_rows() %>%
          janitor::clean_names() %>%
          mutate(across(
            matches("latitud|longitud_wgs84|precio_producto"),
            \(x) as.numeric(gsub(",", ".", x))
          )) %>%
          mutate(dia = dia)
      }) %>%
      bind_rows() %>%
      dbAppendTable(con, "preus", .)
  })

temps <- Sys.time() - temps
dbDisconnect(con)
