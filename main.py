from scraper import scraper


plataformas = {
    "Netflix": "nfx",
    "Filmin": "fil",
    "Planet Horror Amazon Channel": "pha"
}

tiempo_espera = 0

simultaneidad = 3

if __name__ == "__main__":
    scraper.scrapear(plataformas, tiempo_espera, simultaneidad)