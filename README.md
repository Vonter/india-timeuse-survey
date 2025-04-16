# india-timeuse-survey

Pre-processed datasets of time use survey data for India. Sourced from the [Time Use Survey, 2024](https://microdata.gov.in/NADA/index.php/catalog/236).

Explore the dataset using https://india-timeuse-survey.netlify.app/ (thanks to [thedivtagguy](https://github.com/thedivtagguy) for the explorer!)

To browse the old version of the repository for the Time Use Survey, 2019, browse the [2019/](2019) folder.

## Data

* [data/individual_daily_schedule.parquet](data/individual_daily_schedule.parquet): Individual profiles with detailed daily time use schedule.

## Scripts

- [parse.py](parse.py): Parses the raw source data, and generates the individual profiles Parquet dataset

## License

This india-timeuse-survey dataset is made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. 
Some individual contents of the database are under copyright by MoSPI.

You are free:

* **To share**: To copy, distribute and use the database.
* **To create**: To produce works from the database.
* **To adapt**: To modify, transform and build upon the database.

As long as you:

* **Attribute**: You must attribute any public use of the database, or works produced from the database, in the manner specified in the ODbL. For any use or redistribution of the database, or works produced from it, you must make clear to others the license of the database and keep intact any notices on the original database.
* **Share-Alike**: If you publicly use any adapted version of this database, or works produced from an adapted database, you must also offer that adapted database under the ODbL.
* **Keep open**: If you redistribute the database, or an adapted version of it, then you may use technological measures that restrict the work (such as DRM) as long as you also redistribute a version without such measures.

## Generating

Ensure the source CSV datasets are present in the `raw` directory before running the scripts, and that you have `python` installed.

```
# Generate the individual profiles Parquet dataset
python parse.py
```

## TODO

- Document sources for mapping codes to names
- Document a data dictionary
- Dataset with activitywise duration in minutes
- Dataset with aggregation by demographic group
- Add field for region code/name mapping

## Credits

- [MoSPI](https://mospi.gov.in)
