# Explanation

This library was developed because DSS interactions are hard, and it's difficult to exchange multiple CalSim runs with other modelers. It's an in-progress attempt to solve parts of that problem, aiming at dashboard tools that can tolerate the setup headaches in favor of faster and easier data-access.

## SQL Dialect

[`duckdb`](https://duckdb.org/) was selected because it is an [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) SQL database, with [similar optimizations for timeseries data](https://duckdb.org/docs/stable/guides/performance/indexing.html) as DSS files, with the added benefits of being a full-fledged relational database.

Additionally, `duckdb` has good [Python API](https://duckdb.org/docs/stable/clients/python/overview) support, and is a single additional dependency on the project. Other scientific data storage solutions come with other advantages, but also come with other dependencies.

## Roadmap

Note: As this project is still in development, additional features and continued support depends on the utility of the tool (as voiced by CalSim users). 

Some things I would like to improve on:

- Optimization of data insertions to preserve the zone mapping of the `results` table.
- Track multiple source files per run (so SV and DV files can both be added and remembered).

If you have more ideas, please add them to [GitHub Issues](https://github.com/CentralValleyModeling/csdb/issues).
