library(dplyr)
library(purrr)
library(readr)
library(readxl)
library(stringr)
library(tibble)

all_df = list()


### 2019 - 2022

files = list(
  "2022" = "data/raw/enrollment_2223.xlsx",
  "2021" = "data/raw/enrollment_2122.xlsx",
  "2020" = "data/raw/enrollment_2021.xlsx",
  "2019" = "data/raw/enrollment_1920.xlsx"
)

df = files %>%
  purrr::map_dfr(
    readxl::read_excel,
    sheet = "District",
    skip = 2,
    col_types = "text",
    .id = "year"
  ) %>%
  `names<-`(
    names(.) %>%
      gsub(" ", "_", .) %>%
      toupper()
  ) %>%
  dplyr::select(
    YEAR, COUNTY_CODE, COUNTY_NAME, DISTRICT_CODE, DISTRICT_NAME,
    TOTAL_ENROLLMENT
  )

# R has major warts.
all_df = append(all_df, list(df))


### 2017 - 2018

years = c(
  "2018",
  "2017"
)

files = tibble::tribble(
  ~path,                                            ~skip,
  "data/raw/enrollment_1819/EnrollmentReport.xlsx", 2,
  "data/raw/enrollment_1718/enr.xlsx",              1
)

df = files %>%
  purrr::pmap(readxl::read_excel, col_types = "text") %>%
  `names<-`(years) %>%
  purrr::list_rbind(names_to = "YEAR") %>%
  `names<-`(toupper(names(.))) %>%
  dplyr::rename(
    COUNTY_CODE = COUNTY_ID,
    DISTRICT_CODE = DIST_ID,
    TOTAL_ENROLLMENT = ROW_TOTAL
  ) %>%
  dplyr::filter(SCHOOL_NAME == "District Total", PRGCODE == 55) %>%
  dplyr::select(
    YEAR, COUNTY_CODE, COUNTY_NAME, DISTRICT_CODE, DISTRICT_NAME,
    TOTAL_ENROLLMENT
  )

all_df = append(all_df, list(df))


### 2010 - 2016

files = c(
  "2016" = "data/raw/enrollment_1617/enr.xlsx",
  "2015" = "data/raw/enrollment_1516/enr.xlsx",
  "2014" = "data/raw/enrollment_1415/enr.xlsx",
  "2013" = "data/raw/enrollment_1314/enr.xlsx"# ,
  # "2012" = "data/raw/enrollment_1213/enr.xlsx",
  # "2011" = "data/raw/enrollment_1112/enr.xlsx",
  # "2010" = "data/raw/enrollment_1011/enr.xls"
)

df = files %>%
  purrr::map(readxl::read_excel, col_types = "text") %>%
  purrr::map(
    ~{
      .x %>%
        `names<-`(
          names(.x) %>%
            toupper() %>%
            gsub(" ", "_", .) %>%
            gsub("LEA_", "DISTRICT_", .) %>%
            gsub("DIST_", "DISTRICT_", .) %>%
            gsub("_ID", "_CODE", .) %>%
            gsub("CO_CODE", "COUNTY_CODE", .) %>%
            gsub("REDUCED_LUNCH", "REDUCED_PRICE_LUNCH", .)
        )
    }
  ) %>%
  purrr::list_rbind(names_to = "YEAR") %>%
  dplyr::filter(
    grepl("Total", SCHOOL_NAME, ignore.case = TRUE),
    PRGCODE == 55
  ) %>%
  dplyr::rename(TOTAL_ENROLLMENT = ROW_TOTAL) %>%
  dplyr::select(
    YEAR, COUNTY_CODE, COUNTY_NAME, DISTRICT_CODE, DISTRICT_NAME,
    TOTAL_ENROLLMENT
  )

all_df = append(all_df, list(df))


### Combine

all = purrr::list_rbind(all_df) %>%
  dplyr::filter(
    COUNTY_CODE != "End of worksheet",
    # State, county totals not provided for every year. Calculate them.
    DISTRICT_CODE != "9999"
  ) %>%
  dplyr::arrange(COUNTY_CODE, DISTRICT_CODE, YEAR) %>%
  dplyr::mutate(
    dplyr::across(c(YEAR, TOTAL_ENROLLMENT), as.numeric),
    dplyr::across(c(COUNTY_NAME, DISTRICT_NAME), stringr::str_to_title),
    YEAR_LONG = paste(YEAR, YEAR - 2000 + 1, sep = "-")
  ) %>%
  dplyr::relocate(YEAR_LONG, .after = YEAR)

state = all %>%
  dplyr::group_by(YEAR, YEAR_LONG) %>%
  dplyr::summarise(TOTAL_ENROLLMENT = sum(TOTAL_ENROLLMENT)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    COUNTY_CODE   = "00",
    COUNTY_NAME   = "State Total",
    DISTRICT_CODE = "0000",
    DISTRICT_NAME = "State Total"
  ) %>%
  dplyr::arrange(YEAR)

county = all %>%
  dplyr::group_by(YEAR, YEAR_LONG, COUNTY_CODE, COUNTY_NAME) %>%
  dplyr::summarise(TOTAL_ENROLLMENT = sum(TOTAL_ENROLLMENT)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    DISTRICT_CODE = "9999",
    DISTRICT_NAME = "County Total"
  ) %>%
  dplyr::arrange(COUNTY_CODE, YEAR)

all = all %>%
  dplyr::bind_rows(state, county) %>%
  dplyr::group_by(COUNTY_CODE, DISTRICT_CODE) %>%
  dplyr::mutate(
    CHG = TOTAL_ENROLLMENT - dplyr::lag(TOTAL_ENROLLMENT, order_by = YEAR)
  ) %>%
  dplyr::ungroup()

n_row_before = nrow(all)

all_10y = all %>%
  dplyr::group_by(COUNTY_CODE, DISTRICT_CODE) %>%
  dplyr::mutate(n = dplyr::n()) %>%
  dplyr::filter(n == 10) %>%
  dplyr::mutate(
    CHG_10Y = dplyr::if_else(
      YEAR == max(YEAR),
      TOTAL_ENROLLMENT[YEAR == max(YEAR)] - TOTAL_ENROLLMENT[YEAR == min(YEAR)],
      NA_real_
    ),
    PCT_CHG_10Y = dplyr::if_else(
      YEAR == max(YEAR),
      (TOTAL_ENROLLMENT[YEAR == max(YEAR)] / TOTAL_ENROLLMENT[YEAR == min(YEAR)]) - 1,
      NA_real_
    )
  ) %>%
  dplyr::ungroup() %>%
  dplyr::select(YEAR, COUNTY_CODE, DISTRICT_CODE, CHG_10Y, PCT_CHG_10Y)

all = all %>%
  dplyr::left_join(all_10y, by = c("YEAR", "COUNTY_CODE", "DISTRICT_CODE"))

n_row_after = nrow(all)

stopifnot(n_row_before == n_row_after)

readr::write_csv(all, "data/clean/enrollment_2013-14_2022-23.csv")
