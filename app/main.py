#Importamos todas las librerías y los modulos necesarios

from fastapi import FastAPI
import sqlalchemy as sql
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Table
from sqlalchemy import func, select, cast, Numeric, case

# Declaramos la variable para FastAPI
app = FastAPI(
    title= "Directorio Peliculas y Series",
    description= "Netflix, Hulu, Amazon Prime Video y Disney",
    version="1.0.1",
    openapi_tags=[{
        "name": "Queries",
        "description": "Peliculas y Series"
    }]
)

# Creamos la conexión a la base de datos y la guardamos en una variable
engine = sql.create_engine('mysql+pymysql://root:Mies2019!!@host.docker.internal:3306/proyecto_individual1')

# MetaData es es un modulo de sqlalchemy que sirve para trabajar las características de una base de datos (tablas, colunas, etc)
meta_data = sql.MetaData(bind=engine)
sql.MetaData.reflect(meta_data)

actors = meta_data.tables['actors']
directorio = meta_data.tables['directorio_peliculas_series']

# Definimos los get de la api
@app.get('/get_max_duration', tags=['Queries'])
def get_max_duration(anio:int, plat:str, type:str):
    if (type == "min"):
        type = "Movie"
    else:
        type = "Tv Show"
    with engine.connect() as conn:
        result = select(directorio.c.title, directorio.c.duration).where(directorio.c.release_year == anio).where(directorio.c.platform == plat).where(directorio.c.type == type).order_by(cast(directorio.c.duration, Numeric(3,0)).desc())
        return conn.execute(result).first()


@app.get('/get_count_platform', tags=['Queries'])
def get_count_platform(plat:str):
    with engine.connect() as conn:
        result = select(directorio.c.platform, func.sum(case([(directorio.c.type == 'Movie', 1)])).label('Movie'), func.sum(case([(directorio.c.type == 'TV Show', 1)])).label('TV Show')).where(directorio.c.platform == plat).group_by(directorio.c.platform)
        return conn.execute(result).first()


@app.get('/get_listedin', tags=['Queries'])
def get_listedin(genero:str):
    with engine.connect() as conn:
        result = select(directorio.c.platform, func.count().label('cantidad')).filter(directorio.c.listed_in.like("%"+genero+"%")).group_by(directorio.c.platform).order_by(func.count().desc())
        return conn.execute(result).first()


@app.get('/get_actor', tags=['Queries'])
def get_actor(plat:str, anio:int):
    with engine.connect() as conn:
        result = select(actors.c.platform, func.count().label('cantidad'), actors.c.cast.label('actor')).where(actors.c.platform == plat).where(actors.c.release_year == anio).where(actors.c.cast != 'sin datos').group_by(actors.c.cast).order_by(func.count().desc())
        return conn.execute(result).first()
