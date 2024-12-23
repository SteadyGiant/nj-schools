library(dplyr)
library(purrr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

options(scipen = 999)

all_df = list()


### 2019 - 2023

files = list(
  "2023" = "data/raw/enrollment_2324.xlsx",
  "2022" = "data/raw/enrollment_2223.xlsx",
  "2021" = "data/raw/enrollment_2122.xlsx",
  "2020" = "data/raw/enrollment_2021.xlsx",
  "2019" = "data/raw/enrollment_1920.xlsx"
)

df = files %>%
  purrr::map(
    readxl::read_excel, sheet = "District", skip = 2, col_types = "text"
  ) %>%
  # The 2019-20 sheet has a typo in a column name we need.
  purrr::map_dfr(
    ~{
      .x %>%
        `names<-`(
          names(.x) %>%
            gsub("Pre -K", "Pre-K", .) %>%
            gsub(" ", "_", .) %>%
            toupper()
        )
    },
    .id = "YEAR"
  ) %>%
  dplyr::select(
    YEAR,
    COUNTY_CODE,
    COUNTY_NAME,
    DISTRICT_CODE,
    DISTRICT_NAME,
    PK12_ENROLLMENT = TOTAL_ENROLLMENT,
    PK_FULL_ENROLLMENT = `PRE-K_FULLDAY`,
    PK_HALF_ENROLLMENT = `PRE-K_HALFDAY`
  )

# R has major warts.
all_df = append(all_df, list(df))


### 2017 - 2018

# See COUNTY_NAME Agency DISTRICT_CODE 1431 DISTRICT_NAME Katzenbach. Same
# COUNTY_CODE 21 as Mercer.

files = tibble::tribble(
  ~year,  ~path,                                            ~skip,
  "2018", "data/raw/enrollment_1819/EnrollmentReport.xlsx", 2,
  "2017", "data/raw/enrollment_1718/enr.xlsx",              1
)

df = purrr::map2(
  .x = files$path,
  .y = files$skip,
  ~readxl::read_excel(.x, skip = .y, col_types = "text")
) %>%
  `names<-`(files$year) %>%
  purrr::list_rbind(names_to = "YEAR") %>%
  `names<-`(toupper(names(.))) %>%
  dplyr::filter(
    SCHOOL_NAME == "District Total", PRGCODE %in% c("55", "PF", "PH")
  ) %>%
  tidyr::pivot_wider(
    id_cols = c(YEAR, COUNTY_ID, COUNTY_NAME, DIST_ID, DISTRICT_NAME),
    # DISTRICT_LEVEL is inconsistent. See DISTRICT_CODE == 3180. When
    # PRGCODE == PF I expect that GRADE_LEVEL == PK. Not true for some
    # districts! Instead, GRADE_LEVEL == Total.
    names_from = PRGCODE,
    values_from = ROW_TOTAL
  ) %>%
  dplyr::rename(
    COUNTY_CODE        = COUNTY_ID,
    DISTRICT_CODE      = DIST_ID,
    PK12_ENROLLMENT    = `55`,
    PK_FULL_ENROLLMENT = PF,
    PK_HALF_ENROLLMENT = PH
  )

all_df = append(all_df, list(df))


### 2010 - 2016

files = c(
  "2016" = "data/raw/enrollment_1617/enr.xlsx",
  "2015" = "data/raw/enrollment_1516/enr.xlsx",
  "2014" = "data/raw/enrollment_1415/enr.xlsx",
  "2013" = "data/raw/enrollment_1314/enr.xlsx",
  "2012" = "data/raw/enrollment_1213/enr.xlsx"
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
    SCHOOL_CODE == "999",
    PRGCODE %in% c("55", "PF", "PH")
  ) %>%
  tidyr::pivot_wider(
    id_cols = c(YEAR, COUNTY_CODE, COUNTY_NAME, DISTRICT_CODE, DISTRICT_NAME),
    names_from = PRGCODE,
    values_from = ROW_TOTAL
  ) %>%
  dplyr::rename(
    PK12_ENROLLMENT    = `55`,
    PK_FULL_ENROLLMENT = PF,
    PK_HALF_ENROLLMENT = PH
  )

all_df = append(all_df, list(df))


### Combine

all = purrr::list_rbind(all_df) %>%
  dplyr::filter(
    !is.na(COUNTY_NAME),
    # State, county totals not provided for every year. Calculate them.
    DISTRICT_CODE != "9999"
  ) %>%
  dplyr::arrange(COUNTY_CODE, DISTRICT_CODE, YEAR) %>%
  # Some districts have no record for PRGCODE == PH, etc. I assume that means
  # they have no enrollment for missing grade levels.
  tidyr::replace_na(
    list(PK_HALF_ENROLLMENT = "0", PK_FULL_ENROLLMENT = "0")
  ) %>%
  dplyr::mutate(
    dplyr::across(c(YEAR, dplyr::ends_with("_ENROLLMENT")), as.numeric),
    dplyr::across(c(COUNTY_NAME, DISTRICT_NAME), stringr::str_to_title),
    # NOTE: Half-day Pre-K seems like FTE. The row sum of all grade enrollments
    # always equals the total enrollment column value.
    PK_ENROLLMENT = PK_FULL_ENROLLMENT + PK_HALF_ENROLLMENT,
    K12_ENROLLMENT = PK12_ENROLLMENT - PK_ENROLLMENT,
    YEAR_LONG = paste(YEAR, YEAR - 2000 + 1, sep = "-")
  ) %>%
  dplyr::relocate(YEAR_LONG, .after = YEAR)

state = all %>%
  dplyr::group_by(YEAR, YEAR_LONG) %>%
  dplyr::summarise(dplyr::across(dplyr::ends_with("_ENROLLMENT"), sum)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    COUNTY_CODE   = "99",
    COUNTY_NAME   = "State Total",
    DISTRICT_CODE = "9999",
    DISTRICT_NAME = "State Total"
  ) %>%
  dplyr::arrange(YEAR)

county = all %>%
  dplyr::group_by(YEAR, YEAR_LONG, COUNTY_CODE, COUNTY_NAME) %>%
  dplyr::summarise(dplyr::across(dplyr::ends_with("_ENROLLMENT"), sum)) %>%
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
    DISTRICT_NAME               = DISTRICT_NAME[YEAR == max(YEAR)],
    CHG_PK12_ENROLLMENT         = PK12_ENROLLMENT - dplyr::lag(PK12_ENROLLMENT, order_by = YEAR),
    PCT_CHG_PK12_ENROLLMENT     = (PK12_ENROLLMENT / dplyr::lag(PK12_ENROLLMENT, order_by = YEAR)) - 1,
    CHG_10YR_PK12_ENROLLMENT    = PK12_ENROLLMENT - dplyr::lag(PK12_ENROLLMENT, n = 10, order_by = YEAR),
    PCT_CHG_10Y_PK12_ENROLLMENT = (PK12_ENROLLMENT / dplyr::lag(PK12_ENROLLMENT, n = 10, order_by = YEAR)) - 1,
    CHG_K12_ENROLLMENT          = K12_ENROLLMENT - dplyr::lag(K12_ENROLLMENT, order_by = YEAR),
    PCT_CHG_K12_ENROLLMENT      = (K12_ENROLLMENT / dplyr::lag(K12_ENROLLMENT, order_by = YEAR)) - 1,
    CHG_10Y_K12_ENROLLMENT      = K12_ENROLLMENT - dplyr::lag(K12_ENROLLMENT, n = 10, order_by = YEAR),
    PCT_CHG_10Y_K12_ENROLLMENT  = (K12_ENROLLMENT / dplyr::lag(K12_ENROLLMENT, n = 10, order_by = YEAR)) - 1
  ) %>%
  dplyr::ungroup()

rm(all_df, county, df, files, state)


##############
### Checks ###
##############

for (year in unique(all$YEAR_LONG)) {
  sum_counties = all %>%
    filter(DISTRICT_NAME == "County Total", YEAR_LONG == year) %>%
    pull(K12_ENROLLMENT) %>%
    sum()

  state_total = all %>%
    filter(DISTRICT_NAME == "State Total", YEAR_LONG == year) %>%
    pull(K12_ENROLLMENT)

  stopifnot(sum_counties == state_total)
}

rm(state_total, sum_counties, year)


##############
### Export ###
##############

readr::write_csv(all, "data/clean/enrollment_2012-13_2023-24.csv")

all %>%
  dplyr::filter(
    YEAR %in% c(2013, 2023),
    COUNTY_NAME != "Charters"
  ) %>%
  dplyr::select(
    YEAR_LONG,
    County             = COUNTY_NAME,
    `School district`  = DISTRICT_NAME,
    `K-12 enrollment`  = K12_ENROLLMENT,
    `% change`         = PCT_CHG_10Y_K12_ENROLLMENT
  ) %>%
  tidyr::pivot_wider(
    id_cols = c(County, `School district`),
    names_from = YEAR_LONG,
    values_from = c(`K-12 enrollment`, `% change`),
    names_sep = ", "
  ) %>%
  dplyr::select(-`% change, 2013-14`) %>%
  dplyr::rename(`% change` = `% change, 2023-24`) %>%
  dplyr::filter(!is.na(`% change`)) %>%
  readr::write_csv("data/clean/enrollment_2012-13_2013-24__condensed.csv")
