studios = {
    "aachen": [
        "Städteregion Aachen",
        "Düren",
        "Heinsberg",
        "Euskirchen",
    ],
    "koeln": [
        "Köln",
        "Leverkusen",
        "Oberbergischer Kreis",
        "Rheinisch-Bergischer Kreis",
        "Rhein-Erft-Kreis",
        "Rhein-Sieg-Kreis",
    ],
    "muenster": [
        "Münster",
        "Borken",
        "Coesfeld",
        "Steinfurt",
        "Warendorf",
    ],
    "essen": [
        "Bochum",
        "Bottrop",
        "Essen",
        "Gelsenkirchen",
        "Herne",
        "Mülheim an der Ruhr",
        "Oberhausen",
        "Recklinghausen",
    ],
    "wuppertal": [
        "Oberbergischer Kreis",
        "Rheinisch-Bergischer Kreis",
        "Remscheid",
        "Wuppertal",
        "Ennepe-Ruhr-Kreis",
        "Solingen",
        "Mettmann",
    ],
    "suedwestfalen": [
        "Siegen-Wittgenstein",
        "Hochsauerlandkreis",
        "Märkischer Kreis",
        "Olpe",
        "Soest",
    ],
    "duisburg": [
        "Duisburg",
        "Wesel",
        "Kleve",
    ],
    "bonn": [
        "Bonn",
        "Euskirchen",
        "Rhein-Sieg-Kreis",
    ],
    "duesseldorf": [
        "Düsseldorf",
        "Krefeld",
        "Mönchengladbach",
        "Rhein-Kreis Neuss",
        "Viersen",
        "Mettmann",
    ],
    "dortmund": [
        "Dortmund",
        "Hagen",
        "Hamm",
        "Unna",
        "Ennepe-Ruhr-Kreis",
    ],
    "bielefeld": [
        "Bielefeld",
        "Gütersloh",
        "Herford",
        "Höxter",
        "Lippe",
        "Minden-Lübbecke",
        "Paderborn",
    ]
}

studio_links = {
    "koeln": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-koeln-leverkusen-100.html",
    "bonn":"https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-bonn-rhein-sieg-100.html",
    "muenster": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-muensterland-100.html",
    "duisburg": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-niederrhein-100.html",
    "aachen":"https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-aachen-dueren-heinsberg-euskirchen-100.html",
    "suedwestfalen": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-siegen-sauerland-olpe-100.html",
    "duesseldorf": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-duesseldorf-krefeld-moenchengladbach-100.html",
    "essen": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-essen-bochum-gelsenkirchen-100.html",
    "wuppertal": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-wuppertal-bergisches-land-100.html",
    "bielefeld": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-bielefeld-ostwestfalen-paderborn-100.html",
    "dortmund": "https://www1.wdr.de/nachrichten/themen/coronavirus/corona-virus-dortmund-hagen-hamm-recklinghausen-unna-100.html",
}

link_for_district = {}

for studio, districts in studios.items():
    for district in districts:
        link_for_district[district] = studio_links[studio]

link_for_district['Gesamt'] = "https://www1.wdr.de/nachrichten/themen/coronavirus/ticker-corona-virus-nrw-100.html"
