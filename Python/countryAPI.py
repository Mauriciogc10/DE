### code we implemented a filter to only retrieve the countries in Asia
import requests
import json

# Define the URL of the API
url = "https://restcountries.com/v3.1/all"

# Send a GET request to the API
response = requests.get(url)
limit = 1

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    countries_data = response.json()
    # Define the continent to filter by (e.g., "Asia")
    target_continent = "Asia"
    
    # Filter countries by continent
    filtered_countries = [country for country in countries_data if target_continent in country.get("continents", [])]
    limited_countries_data = filtered_countries[:limit]
    # Print the data (or process it as needed)
    print(json.dumps(limited_countries_data, indent=4))
else:
    print(f"Failed to retrieve data: {response.status_code}")

[
    {
        "name": {
            "common": "Taiwan",
            "official": "Republic of China (Taiwan)",
            "nativeName": {
                "zho": {
                    "official": "\u4e2d\u83ef\u6c11\u570b",
                    "common": "\u53f0\u7063"
                }
            }
        },
        "tld": [
            ".tw",
            ".\u53f0\u7063",
            ".\u53f0\u6e7e"
        ],
        "cca2": "TW",
        "ccn3": "158",
        "cca3": "TWN",
        "cioc": "TPE",
        "independent": false,
        "status": "officially-assigned",
        "unMember": false,
        "currencies": {
            "TWD": {
                "name": "New Taiwan dollar",
                "symbol": "$"
            }
        },
        "idd": {
            "root": "+8",
            "suffixes": [
                "86"
            ]
        },
        "capital": [
            "Taipei"
        ],
        "altSpellings": [
            "TW",
            "T\u00e1iw\u0101n",
            "Republic of China",
            "\u4e2d\u83ef\u6c11\u570b",
            "Zh\u014dnghu\u00e1 M\u00edngu\u00f3",
            "Chinese Taipei"
        ],
        "region": "Asia",
        "subregion": "Eastern Asia",
        "languages": {
            "zho": "Chinese"
        },
        "translations": {
            "ara": {
                "official": "\u062c\u0645\u0647\u0648\u0631\u064a\u0629 \u0627\u0644\u0635\u064a\u0646 (\u062a\u0627\u064a\u0648\u0627\u0646)",
                "common": "\u062a\u0627\u064a\u0648\u0627\u0646"
            },
            "bre": {
                "official": "Republik Sina (Taiwan)",
                "common": "Taiwan"
            },
            "ces": {
                "official": "\u010c\u00ednsk\u00e1 republika",
                "common": "Tchaj-wan"
            },
            "cym": {
                "official": "Republic of China (Taiwan)",
                "common": "Taiwan"
            },
            "deu": {
                "official": "Republik China (Taiwan)",
                "common": "Taiwan"
            },
            "est": {
                "official": "Taiwani",
                "common": "Taiwan"
            },
            "fin": {
                "official": "Kiinan tasavalta",
                "common": "Taiwan"
            },
            "fra": {
                "official": "R\u00e9publique de Chine (Ta\u00efwan)",
                "common": "Ta\u00efwan"
            },
            "hrv": {
                "official": "Republika Kina",
                "common": "Tajvan"
            },
            "hun": {
                "official": "K\u00ednai K\u00f6zt\u00e1rsas\u00e1g",
                "common": "Tajvan"
            },
            "ita": {
                "official": "Repubblica cinese (Taiwan)",
                "common": "Taiwan"
            },
            "jpn": {
                "official": "\u4e2d\u83ef\u6c11\u56fd",
                "common": "\u53f0\u6e7e"
            },
            "kor": {
                "official": "\uc911\ud654\ubbfc\uad6d",
                "common": "\ub300\ub9cc"
            },
            "nld": {
                "official": "Republiek China (Taiwan)",
                "common": "Taiwan"
            },
            "per": {
                "official": "\u062c\u0645\u0647\u0648\u0631\u06cc \u0686\u06cc\u0646",
                "common": "\u062a\u0627\u06cc\u0648\u0627\u0646"
            },
            "pol": {
                "official": "Republika Chi\u0144ska (Tajwan)",
                "common": "Tajwan"
            },
            "por": {
                "official": "Rep\u00fablica da China",
                "common": "Ilha Formosa"
            },
            "rus": {
                "official": "\u041a\u0438\u0442\u0430\u0439\u0441\u043a\u0430\u044f \u0420\u0435\u0441\u043f\u0443\u0431\u043b\u0438\u043a\u0430",
                "common": "\u0422\u0430\u0439\u0432\u0430\u043d\u044c"
            },
            "slk": {
                "official": "\u010c\u00ednska republika",
                "common": "Taiwan"
            },
            "spa": {
                "official": "Rep\u00fablica de China en Taiw\u00e1n",
                "common": "Taiw\u00e1n"
            },
            "srp": {
                "official": "\u0420\u0435\u043f\u0443\u0431\u043b\u0438\u043a\u0430 \u041a\u0438\u043d\u0430",
                "common": "\u0422\u0430\u0458\u0432\u0430\u043d"
            },
            "swe": {
                "official": "Republiken Kina",
                "common": "Taiwan"
            },
            "tur": {
                "official": "\u00c7in Cumhuriyeti (Tayvan)",
                "common": "Tayvan"
            },
            "urd": {
                "official": "\u062c\u0645\u06c1\u0648\u0631\u06cc\u06c1 \u0686\u06cc\u0646 (\u062a\u0627\u0626\u06cc\u0648\u0627\u0646)",
                "common": "\u062a\u0627\u0626\u06cc\u0648\u0627\u0646"
            }
        },
        "latlng": [
            23.5,
            121.0
        ],
        "landlocked": false,
        "area": 36193.0,
        "demonyms": {
            "eng": {
                "f": "Taiwanese",
                "m": "Taiwanese"
            },
            "fra": {
                "f": "Ta\u00efwanaise",
                "m": "Ta\u00efwanais"
            }
        },
        "flag": "\ud83c\uddf9\ud83c\uddfc",
        "maps": {
            "googleMaps": "https://goo.gl/maps/HgMKFQjNadF3Wa6B6",
            "openStreetMaps": "https://www.openstreetmap.org/relation/449220"
        },
        "population": 23503349,
        "fifa": "TPE",
        "car": {
            "signs": [
                "RC"
            ],
            "side": "right"
        },
        "timezones": [
            "UTC+08:00"
        ],
        "continents": [
            "Asia"
        ],
        "flags": {
            "png": "https://flagcdn.com/w320/tw.png",
            "svg": "https://flagcdn.com/tw.svg"
        },
        "coatOfArms": {
            "png": "https://mainfacts.com/media/images/coats_of_arms/tw.png",
            "svg": "https://mainfacts.com/media/images/coats_of_arms/tw.svg"
        },
        "startOfWeek": "monday",
        "capitalInfo": {
            "latlng": [
                25.03,
                121.52
            ]
        },
        "postalCode": {
            "format": "#####",
            "regex": "^(\\d{5})$"
        }
    }
]

