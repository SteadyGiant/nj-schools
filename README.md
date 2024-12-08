# New Jersey School Data

Tidy data about New Jersey schools.

## Contents

In [`data/clean`](./data/clean)...

- [`enrollment_2012-13_2023-24.csv`](./data/clean/enrollment_2012-13_2023-24.csv) - PK-12 and K-12 Fall enrollment by school district, county, and state (just NJ) for academic years 2012-13 through 2023-24. From the DOE's [Fall Enrollment Reports](https://www.nj.gov/education/doedata/enr/).[^1]
- [`enrollment_2012-13_2022-23.csv`](./data/clean/enrollment_2012-13_2022-23.csv) - The same but up to 2022-23.

At the repo root...

- `main.r` - The script used to create the above datasets.
- `main.ipynb` - A failed attempt to replace `main.r` with Python and [Ibis](https://ibis-project.org/).

***

[^1]: Unfortunately, the school-level data in these reports aren't high quality enough for my purposes, so I didn't clean them. I'll elaborate later.
