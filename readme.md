![1670558605809](https://gmo-research.com/application/files/7816/3894/7276/streaming_platform_image_Shutterstock_s.jpg)

### **PROYECTO 1 INDIVIDUAL HENRY - DATA ENGINEER - ETL**

---



El objetivo de este proyecto es realizar un trabajo de ETL, el consiste en:

* LEER archivos de datos en formatos .csv y .json (en este caso)

```python
df_csv_amazon = pd.read_csv('https://raw.githubusercontent.com/bigdatamartin/PI01_DATA05/main/Datasets/amazon_prime_titles.csv')
df_csv_disney = pd.read_csv('https://raw.githubusercontent.com/bigdatamartin/PI01_DATA05/main/Datasets/disney_plus_titles.csv')
df_csv_hulu = pd.read_csv('https://raw.githubusercontent.com/bigdatamartin/PI01_DATA05/main/Datasets/hulu_titles.csv')
df_json_netflix = pd.read_json('https://raw.githubusercontent.com/bigdatamartin/PI01_DATA05/main/Datasets/netflix_titles.json')
```

- TRANSFORMAR los datos de los archivos
  Normalizando

  ```python
  df_csv_amazon['platform'] = 'Amazon Prime Video'
  df_csv_disney['platform'] = 'Disney Plus'
  df_csv_hulu['platform'] = 'Hulu'
  df_json_netflix['platform'] = 'Netflix'
  ```

    Creando un DataFrame unificado

```
df = pd.concat([df_csv_amazon, df_csv_disney, df_csv_hulu, df_json_netflix])

```

Tratando datos none o null

```
df[['duration']] = df[['duration']].fillna(value='sin datos')
df[['rating']] = df[['rating']].fillna(value='sin datos')  
df[['date_added']] = df[['date_added']].fillna(value='sin datos')
df[['country']] = df[['country']].fillna(value='sin datos')
df[['cast']] = df[['cast']].fillna(value='sin datos')
df[['director']] = df[['director']].fillna(value='sin datos')
```

* CARGAR los datos a una base de datos, una vez finalizado el modelado

```
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:Mies2019!!@localhost:3306/proyecto_individual1')
df_modelado_actors.to_sql('actors', engine, index=False)
df.to_sql('directorio_peliculas_series', engine, index=False)
```



---



Una vez creada la base de datos, se procede a escribir las consultas pedidas en las consignas del PI


* Máxima duración según tipo de film (película/serie), por plataforma y por año: El request debe ser: ***get_max_duration(año, plataforma, [min o season])***
* Cantidad de películas y series (separado) por plataforma El request debe ser: ***get_count_plataform(plataforma)***
* Cantidad de veces que se repite un género y plataforma con mayor frecuencia del mismo. El request debe ser: ***get_listedin('genero')***
  Como ejemplo de género pueden usar 'comedy', el cuál deberia devolverles un cunt de 2099 para la plataforma de amazon.
* Actor que más se repite según plataforma y año. El request debe ser: ***get_actor(plataforma, año)***

---

Para lograr esto se recurre a la creación de una API mediante el framework FastAPI y la importación y utilización de lenguaje de consultas SQLalchemy

```python
from fastapi import FastAPI
import sqlalchemy as sql
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Table
from sqlalchemy import func, select, cast, Numeric, case
```

```python
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
```

---

Finalmente, mediante la dockerizacion de la API, podremos realizar las consultas desde un container sin necesidad de correr el script

*En este paso debemos correr en terminal los siguientes códigos:*

docker build -t myimage .

docker run -d --name mycontainer -p 80:80 myimage

---
