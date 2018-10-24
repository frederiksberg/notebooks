# Vejledning 
Vejledning i hentning af lokalplan-pdf til PostgreSQL
## 1. Klargør tabeller i databasen

Hent alle lokalplaner fra plansystems-WFS til PostgreSQL database:

```bash
ogr2ogr -f PostgreSQL PG:"dbname=xx host=xx port=xx user=xx password=xx" WFS:"http://geoservice.plansystem.dk/wfs?version=1.0.0" pdk:lokalplan -lco SCHEMA=proj_lokalplan_dokument -lco GEOMETRY_NAME=the_geom -nln "lokalplan" -a_srs "EPSG:25832"
```

Lav tabel i databasen til at indsætte planid, status og tilhørende dokumenttekst
```sql
# Lav skema og tabel
create schema elasticsearch;

create table elasticsearch.lokalplan_dokument (
	gid serial primary key not null,
	planid int4,
	plannavn varchar,
	status varchar,
	document text
)

## indsæt plandata ind i lokalplan_dokument
insert into elasticsearch.lokalplan_dokument(planid, plannavn, status)
SELECT planid, plannavn, status
FROM job_plandatadk.lokalplan
where STATUS in ('V', 'F') and AKTUEL = true and komnr = 147 and doklink is not null;
```

## 2. Hent plandoukmenter
Kør Python-script til at opdatere  

## 3. Opdateringer
Lokalplaner kan have følgende statuser - kladde, forslag, vedtage og aflyst. Vi er kun interesseret at oprette, opdatere eller fjerne lokalplandokumenter i ElasticSearch indexet når der sker ændring i planstatus. `status`-kolonnen i plansystem har værdierne:
* `K` - Kladde
* `F` - Forslag
* `V` - Vedtaget
* `A` - Aflyst

Opdateringsfrekvensen er selvfølgelig afgørende for om en plan kan springe en status over og som udgangspunkt kan opdateringen sættes til at ske en gang om måneden. Følgende scenarier gør sig gældende: 
#### Oprettes
* Planer der ændre status fra kladde til forslag
* Planer med planid som ikke allrede eksisterer (springer status over pga. opdateringfrekvensen)
#### Opdateres
* Planer der ændre status fra forslag til vedtaget
#### Slettes
* Planer der ændre status til aflyst

`planid` vil være det samme for en plan der ændre `status`, men i tabellen angives hvilken plan som er gældende i kolonnen `aktuel`. Herunder ses hvordan der kan opsættes view som kan bruges til opdateringen af `lokalplan_dokument` tabellen.

```sql
create view elasticsearch.lokalplan_dokument_update as
with lp as (
	select planid, komnr, status, aktuel, doklink
	from job_plandatadk.lokalplan
), pd as (
	select planid, status
	from elasticsearch.lokalplan_dokument b
)
--UPDATE: Find tabeller hvor der ændringer i status til vedtaget eller forslag
select 'UPDATE' handling, b.planid, b.status gl_status, a.status ny_status, a.doklink
from lp a
join pd b
on a.planid = b.planid
where a.status <> b.status and a.aktuel = true and a.status = 'V'
union
--INSERT: Find ny planer med status forslag eller vedtaget som ikke allerede eksisterer i plandokument tabellen
select 'INSERT' handling, a.planid, b.status gl_status, a.status ny_status, a.doklink
from lp a
left join pd b
on a.planid = b.planid
where a.aktuel = true and a.komnr = 147 and b.planid is null and a.status in('F', 'V')
union
--DELETE: Fjern de rækker i plandokument-tabellen som er ændret til status: Aflyst
select 'DELETE' handling, b.planid, b.status gl_status, a.status ny_status, a.doklink
from lp a
join pd b
on a.planid = b.planid
where a.status = 'A' and a.komnr = 147 and a.aktuel = true; 
```

Herefter kan `lokalplan_dokument_update.py` køres for at opdatere `lokalplan_dokument` tabellen.