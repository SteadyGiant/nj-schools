library(dplyr)
library(purrr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

options(scipen = 999)

all_df = list()


### 2019 - 2022

files = list(
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
    TOTAL_ENROLLMENT,
    PRE_K_FULL_ENROLLMENT = `PRE-K_FULLDAY`,
    PRE_K_HALF_ENROLLMENT = `PRE-K_HALFDAY`
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
    COUNTY_CODE           = COUNTY_ID,
    DISTRICT_CODE         = DIST_ID,
    TOTAL_ENROLLMENT      = `55`,
    PRE_K_FULL_ENROLLMENT = PF,
    PRE_K_HALF_ENROLLMENT = PH
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
    TOTAL_ENROLLMENT      = `55`,
    PRE_K_FULL_ENROLLMENT = PF,
    PRE_K_HALF_ENROLLMENT = PH
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
    list(PRE_K_HALF_ENROLLMENT = "0", PRE_K_FULL_ENROLLMENT = "0")
  ) %>%
  dplyr::mutate(
    dplyr::across(c(YEAR, dplyr::ends_with("_ENROLLMENT")), as.numeric),
    dplyr::across(c(COUNTY_NAME, DISTRICT_NAME), stringr::str_to_title),
    # NOTE: Half-day Pre-K seems like FTE. The row sum of all grade enrollments
    # always equals the total enrollment column value.
    PRE_K_ENROLLMENT = PRE_K_FULL_ENROLLMENT + PRE_K_HALF_ENROLLMENT,
    K_12_ENROLLMENT = TOTAL_ENROLLMENT - PRE_K_ENROLLMENT,
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
    CHG_TOTAL_ENROLLMENT     = TOTAL_ENROLLMENT - dplyr::lag(TOTAL_ENROLLMENT, order_by = YEAR),
    PCT_CHG_TOTAL_ENROLLMENT = (TOTAL_ENROLLMENT / dplyr::lag(TOTAL_ENROLLMENT, order_by = YEAR)) - 1,
    CHG_K_12_ENROLLMENT      = K_12_ENROLLMENT - dplyr::lag(K_12_ENROLLMENT, order_by = YEAR),
    PCT_CHG_K_12_ENROLLMENT  = (K_12_ENROLLMENT / dplyr::lag(K_12_ENROLLMENT, order_by = YEAR)) - 1
  ) %>%
  dplyr::ungroup()

n_row_before = nrow(all)

# Calculate the "10-year change" in enrollment.
# That is, start at 2012-23; ten academic years later, how did enrollment
# change?
all_10y = all %>%
  # Include County Name because of County Code 21 (Mercer) County Name Agency.
  # It causes Mercer Co to be excluded. From the 2017-2019 sheets.
  dplyr::group_by(COUNTY_CODE, COUNTY_NAME, DISTRICT_CODE) %>%
  dplyr::mutate(n = dplyr::n()) %>%
  dplyr::filter(n == 11) %>%
  dplyr::mutate(
    CHG_10Y_TOTAL_ENROLLMENT = dplyr::if_else(
      YEAR == max(YEAR),
      TOTAL_ENROLLMENT[YEAR == max(YEAR)] - TOTAL_ENROLLMENT[YEAR == min(YEAR)],
      NA_real_
    ),
    PCT_CHG_10Y_TOTAL_ENROLLMENT = dplyr::if_else(
      YEAR == max(YEAR),
      (TOTAL_ENROLLMENT[YEAR == max(YEAR)] / TOTAL_ENROLLMENT[YEAR == min(YEAR)]) - 1,
      NA_real_
    ),
    CHG_10Y_K_12_ENROLLMENT = dplyr::if_else(
      YEAR == max(YEAR),
      K_12_ENROLLMENT[YEAR == max(YEAR)] - K_12_ENROLLMENT[YEAR == min(YEAR)],
      NA_real_
    ),
    PCT_CHG_10Y_K_12_ENROLLMENT = dplyr::if_else(
      YEAR == max(YEAR),
      (K_12_ENROLLMENT[YEAR == max(YEAR)] / K_12_ENROLLMENT[YEAR == min(YEAR)]) - 1,
      NA_real_
    )
  ) %>%
  dplyr::ungroup() %>%
  dplyr::select(
    YEAR, COUNTY_CODE, DISTRICT_CODE,
    dplyr::starts_with(c("CHG_10Y", "PCT_CHG_10Y"))
  )

all = all %>%
  dplyr::left_join(all_10y, by = c("YEAR", "COUNTY_CODE", "DISTRICT_CODE"))


##############
### Checks ###
##############

n_row_after = nrow(all)

stopifnot(n_row_before == n_row_after)

for (year in unique(all$YEAR_LONG)) {

  sum_counties = all %>%
    filter(DISTRICT_NAME == "County Total", YEAR_LONG == year) %>%
    pull(K_12_ENROLLMENT) %>%
    sum()

  state_total = all %>%
    filter(DISTRICT_NAME == "State Total", YEAR_LONG == year) %>%
    pull(K_12_ENROLLMENT)

  stopifnot(sum_counties == state_total)

}


##############
### Export ###
##############

readr::write_csv(all, "data/clean/enrollment_2012-13_2022-23.csv")
