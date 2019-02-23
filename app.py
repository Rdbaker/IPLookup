import databases
import sqlalchemy
from starlette.applications import Starlette
from starlette.config import Config
from starlette.responses import JSONResponse
import uvicorn
from webargs_starlette import use_annotations
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import BIGINT, CIDR, REAL

from datetime import date
from marshmallow import Schema, fields, pprint

# Configuration from environment variables or '.env' file.
config = Config('.env')
DATABASE_URL = config('DATABASE_URL')


# Database table definitions.
metadata = sqlalchemy.MetaData()

geo_id_to_name = sqlalchemy.Table(
    "geo_id_to_name",
    metadata,
    sqlalchemy.Column("geoname_id", BIGINT),
    sqlalchemy.Column("locale_code", sqlalchemy.String),
    sqlalchemy.Column("continent_code", sqlalchemy.String),
    sqlalchemy.Column("continent_name", sqlalchemy.String),
    sqlalchemy.Column("country_iso_code", sqlalchemy.String),
    sqlalchemy.Column("country_name", sqlalchemy.String),
    sqlalchemy.Column("subdivision_1_iso_code", sqlalchemy.String),
    sqlalchemy.Column("subdivision_1_name", sqlalchemy.String),
    sqlalchemy.Column("subdivision_2_iso_code", sqlalchemy.String),
    sqlalchemy.Column("subdivision_2_name", sqlalchemy.String),
    sqlalchemy.Column("city_name", sqlalchemy.String),
    sqlalchemy.Column("metro_code", sqlalchemy.String),
    sqlalchemy.Column("time_zone", sqlalchemy.String),
    sqlalchemy.Column("is_in_european_union", sqlalchemy.Boolean),
)


ip_to_geo_id = sqlalchemy.Table(
    "ip_to_geo_id",
    metadata,
    sqlalchemy.Column("network", CIDR),
    sqlalchemy.Column("geoname_id", BIGINT, sqlalchemy.ForeignKey("geo_id_to_name.geoname_id")),
    sqlalchemy.Column("registered_country_geoname_id", BIGINT),
    sqlalchemy.Column("represented_country_geoname_id", BIGINT),
    sqlalchemy.Column("is_anonymous_proxy", sqlalchemy.Boolean),
    sqlalchemy.Column("is_satellite_provider", sqlalchemy.Boolean),
    sqlalchemy.Column("postal_code", sqlalchemy.String),
    sqlalchemy.Column("latitude", REAL),
    sqlalchemy.Column("longitude", REAL),
    sqlalchemy.Column("accuracy_radius", BIGINT),
)


class IPv4Network(fields.Field):
    """Field that serializes to a title case string and deserializes
    to a lower case string.
    """
    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return ''
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return str(value)



class GeoIPSchema(Schema):
    network = IPv4Network()
    registered_country_geoname_id = fields.Integer()
    represented_country_geoname_id = fields.Integer(nullable=True)
    is_anonymous_proxy = fields.Boolean()
    is_satellite_provider = fields.Boolean()
    postal_code = fields.Str()
    latitude = fields.Float()
    longitude = fields.Float()
    accuracy_radius = fields.Integer()
    geoname_id = fields.Integer()
    locale_code = fields.Str()
    continent_code = fields.Str()
    continent_name = fields.Str()
    country_iso_code = fields.Str()
    subdivision_1_iso_code = fields.Str()
    subdivision_1_iso_name = fields.Str()
    subdivision_2_iso_code = fields.Str()
    subdivision_2_iso_name = fields.Str()
    city_name = fields.Str()
    metro_code = fields.Str()
    time_zone = fields.Str()
    is_in_european_union = fields.Boolean()


GEO_IP_SCHEMA = GeoIPSchema()

# Main application code.
database = databases.Database(DATABASE_URL)
app = Starlette()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.route('/')
@use_annotations(locations=("query",))
async def user(request, ip: str = None):
    requested_ip = ip or request.headers.get('ip')
    if requested_ip is None:
        return JSONResponse({'error': 'No IP address provided'})
    where_clause = ip_to_geo_id.c.network.op('>>=')(requested_ip)
    joined = ip_to_geo_id.join(geo_id_to_name, geo_id_to_name.c.geoname_id == ip_to_geo_id.c.geoname_id)
    query = select([ip_to_geo_id, geo_id_to_name]).select_from(joined).where(where_clause)
    result = await database.fetch_one(query)
    row = GEO_IP_SCHEMA.load(result._row)
    return JSONResponse({'data': row.data, 'errors': row.errors})


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
