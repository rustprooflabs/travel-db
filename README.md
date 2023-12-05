# Travel Database

This project is an example travel database using Postgres and
PostGIS.

## Database structure

The parent structure is the "trip".  Each trip has one or more "trip steps", each with its own time frame and travel mode.  The aggregated geometry (line) data is stored in the `travel.trip_step` table.  The
detailed data is in the `travel.trip_point` table, which is best
queried through the `travel.trip_point_detail` view.

Data is loaded to these tables from the `staging` schema using the function
`travel.load_bad_elf_points(trip_id)`.  As the name implies, this import function
is scoped to data from a [Bad Elf GNSS Surveyor](https://bad-elf.com/pages/be-gps-3300-detail)
(now discontinued).

The `travel.import_duplicate_cleanup` table is the beginning of tracking
data quality issues in the data.

> A blog post showing the use of this database is coming soon!
 

## Deploy schema

Uses sqitch.

```bash
cd ~/git/travel-db/db
sqitch deploy db:pg:travel
```


