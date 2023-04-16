# JustWatch Scraper
## Importante
La finalidad de este script es puramente recreativa. Las ejecuciones siempre han sido a pequeña escala y sin pretensiones. Esto significa que no se han investigado los límites, ni el comportamiento del servidor. En el caso de que se utilice con mayor profundidad se facilitan ciertos ajustes para que su uso siempre sea de manera responsable y sensata. \
Parece que la web de JustWatch solo devuelve los títulos populares en los listados. Si se intenta scraper Netflix, aunque pone casi 9000 títulos, devuelve menos de 2000.\
**Investigar si se puede recuperar completamente**

### Preparación
Antes de ejecutar el script debe cumplimentarse los siguientes aspectos en el archivo main.py
- Diccionario con las plataformas a escrapear. La relación aparece en plataformas.json
```python
plataformas = {
    "Netflix": "nfx",
    "Filmin": "fil",
    "Planet Horror Amazon Channel": "pha"
}
```

- Segundos que esperará entre cada petición
```python
timepo_espera = 0
```

- Simultaneidad. Para la paginación se devuelve un código en cada petición, por lo que esta se debe hacer secuencialmente. Con simultaneidad se refiere al número de plataformas que se analizarán simultáneamente.
```python
simultaneidad = 3
```
### Ejecución
Una vez procesado el script se creará en la carpeta raiz un archivo sqlite3 con los resultados del scrapeo.

### Mantenimiento
Las claves/valores de las plataformas pueden obtenerse ejecutando en la consola del navegador en una página de un proveedor:
```javascript
recopilacion = {}
for (proveedor of window.__DATA__.state.constant.providers) {
    recopilacion[proveedor.clear_name] = proveedor.short_name
}
console.log(JSON.stringify(recopilacion, null, 4)) 
```
Las relación entre el nombre de un género y su abreviación puede obtenerse ejecutando en la consola del navegador en una página de proveedor en español:
```javascript
recopilacion = {}
for (genero of window.__DATA__.state.constant.genres) {
    recopilacion[genero.short_name] = genero.translation
}
console.log(JSON.stringify(recopilacion, null, 4))
```
El pregijo para justwatch_url en la base de datos es:
**https://www.justwatch.com**

Para el poster se debe tener en cuenta:
- prefijo: https://images.justwatch.com
- profile:
    - s166
    - s592
- extension:
    - .webp
    - .jpg