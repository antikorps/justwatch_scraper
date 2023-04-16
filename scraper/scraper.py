from dataclasses import dataclass
from typing import Dict
from pathlib import Path
import sqlite3
import time
import requests
import concurrent.futures
import json
import logging
from multiprocessing import Lock


def obtener_nombre_genero(genero_abreviado: str) -> str:
    relacion_generos = {
        "act":"Acción & Aventura",
        "ani":"Animación",
        "cmy":"Comedia",
        "crm":"Crimen",
        "doc":"Documental",
        "drm":"Drama",
        "fnt":"Fantasía",
        "hst":"Historia",
        "hrr":"Terror",
        "fml":"Familia",
        "msc":"Música",
        "trl":"Misterio & Suspense",
        "rma":"Romance",
        "scf":"Ciencia ficción",
        "spt":"Deporte",
        "war":"Guerra",
        "wsn":"Western",
        "rly":"Reality TV",
        "eur":"Europeas"}
    try:
        return relacion_generos[genero_abreviado]
    except:
        return genero_abreviado

@dataclass
class Resumen:
    plataforma: str
    errores: bool
    errores_mensaje: str
    registros: int


@dataclass
class Justwatch:
    conexion: sqlite3.Connection
    cursor: sqlite3.Cursor
    plataformas: Dict[str, str]
    ruta_principal: str
    tiempo_espera: int
    simultaneidad: int
    sesion = requests.Session()
    cabeceras = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0',
        'Accept': '*/*',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3'
    }
    cerrojo = Lock()
    exito = True

    def preparar_base_datos(self):
        crear_tabla = """
        
        CREATE TABLE registros (
            id INTEGER PRIMARY KEY,
            plataforma TEXT,
            titulo TEXT,
            titulo_original TEXT,
            tipo TEXT,
            justwatch_url TEXT,
            generos TEXT,
            sinopsis TEXT,
            duracion TEXT,
            lanzamiento TEXT,
            creditos TEXT,
            puntuacion_imdb FLOAT,
            puntuacion_tmdb FLOAT,
            poster TEXT,
            estreno INTEGER,
            temporadas INTEGER
        )
        
        """

        self.cursor.execute(crear_tabla)

    def iniciar(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.simultaneidad) as manejador:
            promesas = []
            for plataforma_nombre, plataforma_identificador in self.plataformas.items():
                promesas.append(manejador.submit(self.realizar_peticiones, plataforma_nombre, plataforma_identificador)) 

            for resultado in concurrent.futures.as_completed(promesas):
                resumen: Resumen = resultado.result()
                if resumen.errores:
                    self.exito = False
                    errores = resumen.errores_mensaje.split("\n")
                    for error in errores:
                        logging.error(error)
                else:
                    print(f"{resumen.plataforma} escreapeada correctamente :)")

    def realizar_peticiones(self, plataforma_nombre, plataforma_identificador) -> Resumen:
        avanzar = True
        paginador = ""
        contador = 0

        resumen = Resumen(plataforma_nombre, False, "", 0)
        while avanzar:
            json_data = {
                'operationName': 'GetPopularTitles',
                'variables': {
                    'popularTitlesSortBy': 'ALPHABETICAL',
                    'first': 50,
                    'sortRandomSeed': 0,
                    'popularAfterCursor': paginador,
                    'popularTitlesFilter': {
                        'ageCertifications': [],
                        'excludeGenres': [],
                        'excludeProductionCountries': [],
                        'genres': [],
                        'objectTypes': [],
                        'productionCountries': [],
                        'packages': [
                            plataforma_identificador,
                        ],
                        'excludeIrrelevantTitles': False,
                        'presentationTypes': [],
                        'monetizationTypes': [],
                    },
                    'genres': [],
                    'language': 'es',
                    'country': 'ES',
                },
                'query': """
                query GetPopularTitles($country: Country!, $popularTitlesFilter: TitleFilter, $popularAfterCursor: String, $popularTitlesSortBy: PopularTitlesSorting! = POPULAR, $first: Int! = 40, $language: Language!, $sortRandomSeed: Int! = 0) 
                {
                    popularTitles(
                        country: $country
                        filter: $popularTitlesFilter
                        after: $popularAfterCursor
                        sortBy: $popularTitlesSortBy
                        first: $first
                        sortRandomSeed: $sortRandomSeed
                    ) 
                    {
                        totalCount
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        edges {
                            ...PopularTitleGraphql
                        }
                    }
                }

                fragment PopularTitleGraphql on PopularTitlesEdge {
                    cursor
                    node {
                        objectType content(country: $country, language: $language) {
                            title
                            originalTitle
                            fullPath
                            genres {
                                shortName
                            } 
                            shortDescription
                            runtime
                            originalReleaseDate
                            credits {
                                role
                                name
                                characterName
                                personId
                            } 
                            scoring {
                                imdbScore
                                tmdbScore
                            }
                            posterUrl
                            isReleased
                        }
                        ... on Show {
                            totalSeasonCount
                        }
                    }
                }
            """
            }

            try:
                time.sleep(self.tiempo_espera)
                peticion = self.sesion.post('https://apis.justwatch.com/graphql', headers=self.cabeceras, json=json_data, timeout=12)
                if peticion.status_code != 200:
                    resumen.errores = True
                    resumen.errores_mensaje = f"\nplataforma {plataforma_nombre} paginador {paginador} => status code incorrecto {peticion.status_code}"
                    return resumen
                respuesta = peticion.json()
            except Exception as error:
                resumen.errores = True
                resumen.errores_mensaje = f"plataforma {plataforma_nombre} paginador {paginador} => {error}"
                return resumen
            

            # Registros
            claves_respuesta = respuesta["data"]["popularTitles"].keys()
            if "edges" in claves_respuesta:
                contador += len(respuesta["data"]["popularTitles"]["edges"])
                for registro in respuesta["data"]["popularTitles"]["edges"]:
                    
                    titulo = registro.get("node").get("content").get("title")
                    titulo_original = registro.get("node").get("content").get("originalTitle")
                    
                    tipo = registro.get("node").get("objectType")
                    tipos = {
                        "SHOW": "tv",
                        "MOVIE": "film"
                    }
                    try:
                        tipo = tipos[registro["node"]["objectType"]]
                    except:
                        tipo = None

                    url = registro.get("node").get("content").get("fullPath")
                    

                    generos = []
                    if "genres" in registro["node"]["content"].keys():
                        for genero in registro["node"]["content"]["genres"]:
                            nombre_genero = obtener_nombre_genero(genero["shortName"])
                            generos.append(nombre_genero)
                        generos = json.dumps(generos, ensure_ascii=False)
                    
                    sinopsis = registro.get("node").get("content").get("shortDescription")
                    
                    duracion = registro.get("node").get("content").get("runtime")
                    
                    lanzamiento = registro.get("node").get("content").get("originalReleaseDate")
                   
                    creditos = ""
                    if "credits" in registro["node"]["content"].keys():
                        creditos = json.dumps(registro["node"]["content"]["credits"], ensure_ascii=False)

                    puntuacion_imdb = registro.get("node").get("content").get("scoring").get("imdbScore") 

                    puntuacion_tmdb = registro.get("node").get("content").get("scoring").get("tmdbScore") 
                   
                    poster = registro.get("node").get("content").get("posterUrl") 
                    
                    estreno = registro.get("node").get("content").get("isReleased") 
                    if estreno != None:
                        if estreno == True:
                            estreno = 1
                        else:
                            estreno = 0

                    numero_temporadas = registro.get("node").get("totalSeasonCount")

                    insert_into = """
                    
                    INSERT INTO registros (
                        plataforma,
                        titulo,
                        titulo_original,
                        tipo,
                        justwatch_url,
                        generos,
                        sinopsis,
                        duracion,
                        lanzamiento,
                        creditos,
                        puntuacion_imdb,
                        puntuacion_tmdb,
                        poster,
                        estreno,
                        temporadas
                    )

                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    
                    """

                    try:
                        self.cerrojo.acquire()
                        self.cursor.execute(insert_into, 
                                            (plataforma_nombre, 
                                             titulo,
                                             titulo_original,
                                             tipo,
                                             url,
                                             generos,
                                             sinopsis,
                                             duracion,
                                             lanzamiento,
                                             creditos,
                                             puntuacion_imdb,
                                             puntuacion_tmdb,
                                             poster,
                                             estreno,
                                             numero_temporadas))
                        self.cerrojo.release()

                    except Exception as error:
                        resumen.errores = True
                        resumen.errores_mensaje += f"\nplataforma {plataforma_nombre} paginador {paginador} => {error}"

                    if respuesta.get("data").get("popularTitles").get("totalCount") != None:
                        resumen.registros = respuesta["data"]["popularTitles"]["totalCount"]

            mensaje_avance = f"progreso: {plataforma_nombre}: {contador}"
            print(mensaje_avance)

            identificador_siguiente = respuesta.get("data").get("popularTitles").get("pageInfo").get("hasNextPage")
            if identificador_siguiente != None:
                tiene_siguiente = respuesta["data"]["popularTitles"]["pageInfo"]["hasNextPage"]
                if tiene_siguiente == False:
                    return resumen
                else: 
                    paginador = respuesta["data"]["popularTitles"]["pageInfo"]["endCursor"]
            else:
                resumen.errores = True
                resumen.errores_mensaje += f"\nplataforma {plataforma_nombre} paginador {paginador} => carece de hasNextPage"
                return resumen

def scrapear(plataformas:Dict[str, str], tiempo_espera: int, simultaneidad: int):
    ruta_archivo = Path(__file__).parent.parent
    ruta_base_datos = ruta_archivo / f"resultados_{int(time.time())}.sqlite3"

    conexion = sqlite3.connect(ruta_base_datos, check_same_thread=False)
    cursor = conexion.cursor()

    justwatch = Justwatch(conexion, cursor, plataformas, ruta_archivo, tiempo_espera, simultaneidad)
    justwatch.preparar_base_datos()
    justwatch.iniciar()
        
    conexion.commit()
    
    if justwatch.exito:
        exit(0)
    exit(1)