# coding=utf-8

#  Copyright (c) 2001-2018, Canal TP and/or its affiliates. All rights reserved.
#
# This file is part of Navitia,
#     the software to build cool stuff with public transport.
#
# Hope you'll enjoy and contribute to this project,
#     powered by Canal TP (www.canaltp.fr).
# Help us simplify mobility and open public transport:
#     a non ending quest to the responsive locomotion way of traveling!
#
# LICENCE: This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Stay tuned using
# twitter @navitia
# IRC #navitia on freenode
# https://groups.google.com/d/forum/navitia
# www.navitia.io
import navitia_response

response = navitia_response.NavitiaResponse()

response.queries = [
    'vehicle_journeys/?depth=2&since=20121120T120100+0000&headsign=9580&show_codes=true&until=20121120T214600+0000'
]

response.response_code = 200

response.json_response = """{
  "pagination": {
    "start_page": 0,
    "items_on_page": 1,
    "items_per_page": 25,
    "total_result": 1
  },
  "links": [
    {
      "href": "http://localhost:5001/v1/coverage/stif/stop_points/{stop_point.id}",
      "type": "stop_point",
      "rel": "stop_points",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/stop_areas/{stop_area.id}",
      "type": "stop_area",
      "rel": "stop_areas",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/journey_patterns/{journey_pattern.id}",
      "type": "journey_pattern",
      "rel": "journey_patterns",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/routes/{route.id}",
      "type": "route",
      "rel": "routes",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/journey_pattern_points/{journey_pattern_point.id}",
      "type": "journey_pattern_point",
      "rel": "journey_pattern_points",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/vehicle_journeys/{vehicle_journeys.id}",
      "type": "vehicle_journeys",
      "rel": "vehicle_journeys",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/trips/{trip.id}",
      "type": "trip",
      "rel": "trips",
      "templated": true
    },
    {
      "href": "http://localhost:5001/v1/coverage/stif/vehicle_journeys?depth=2&since=20121120T115800%2B0000&until=20121120T214630%2B0000&headsign=9580",
      "type": "first",
      "templated": false
    }
  ],
  "disruptions": [],
  "feed_publishers": [],
  "context": {
    "timezone": "Europe/Paris",
    "current_datetime": "20190515T111555"
  },
  "vehicle_journeys": [
    {
      "codes": [],
      "name": "9580",
      "journey_pattern": {
        "route": {
          "direction": {
            "embedded_type": "stop_area",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-751008-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87751008"
                },
                {
                  "type": "external_code",
                  "value": "OCE87751008"
                }
              ],
              "name": "gare de Marseille-St-Charles",
              "links": [],
              "coord": {
                "lat": "43.30273",
                "lon": "5.380659"
              },
              "label": "gare de Marseille-St-Charles (Marseille)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87751008"
            },
            "quality": 0,
            "name": "gare de Marseille-St-Charles (Marseille)",
            "id": "stop_area:OCE:SA:87751008"
          },
          "name": "Frankfurt-am-Main-Hbf vers Marseille-St-Charles",
          "links": [],
          "is_frequence": "False",
          "geojson": {
            "type": "MultiLineString",
            "coordinates": []
          },
          "direction_type": "forward",
          "id": "route:OCE:TGV-80110684-87751008-1"
        },
        "id": "journey_pattern:7491",
        "name": "journey_pattern:7491"
      },
      "disruptions": [],
      "calendars": [
        {
          "exceptions": [
            {
              "type": "remove",
              "datetime": "20121201"
            }
          ],
          "active_periods": [
            {
              "begin": "20121029",
              "end": "20121208"
            }
          ],
          "week_pattern": {
            "monday": true,
            "tuesday": true,
            "friday": true,
            "wednesday": true,
            "thursday": true,
            "sunday": true,
            "saturday": true
          }
        }
      ],
      "stop_times": [
        {
          "stop_point": {
            "name": "gare de Frankfurt-am-Main-Hbf",
            "links": [],
            "coord": {
              "lat": "0",
              "lon": "0"
            },
            "label": "gare de Frankfurt-am-Main-Hbf",
            "equipments": [],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-80110684",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0080-110684-00"
                },
                {
                  "type": "UIC8",
                  "value": "80110684"
                },
                {
                  "type": "external_code",
                  "value": "OCE80110684"
                }
              ],
              "name": "gare de Frankfurt-am-Main-Hbf",
              "links": [],
              "coord": {
                "lat": "0",
                "lon": "0"
              },
              "label": "gare de Frankfurt-am-Main-Hbf",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:80110684"
            }
          },
          "utc_arrival_time": "130100",
          "utc_departure_time": "130100",
          "headsign": "9580",
          "arrival_time": "140100",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71093"
          },
          "departure_time": "140100"
        },
        {
          "stop_point": {
            "name": "gare de Mannheim-Hbf",
            "links": [],
            "coord": {
              "lat": "0",
              "lon": "0"
            },
            "label": "gare de Mannheim-Hbf",
            "equipments": [],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-80140087",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0080-140087-BV"
                },
                {
                  "type": "UIC8",
                  "value": "80140087"
                },
                {
                  "type": "external_code",
                  "value": "OCE80140087"
                }
              ],
              "name": "gare de Mannheim-Hbf",
              "links": [],
              "coord": {
                "lat": "0",
                "lon": "0"
              },
              "label": "gare de Mannheim-Hbf",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:80140087"
            }
          },
          "utc_arrival_time": "133700",
          "utc_departure_time": "134000",
          "headsign": "9580",
          "arrival_time": "143700",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71094"
          },
          "departure_time": "144000"
        },
        {
          "stop_point": {
            "name": "gare de Karlsruhe-Hbf",
            "links": [],
            "coord": {
              "lat": "0",
              "lon": "0"
            },
            "label": "gare de Karlsruhe-Hbf",
            "equipments": [],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-80142281",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0080-142281-BV"
                },
                {
                  "type": "UIC8",
                  "value": "80142281"
                },
                {
                  "type": "external_code",
                  "value": "OCE80142281"
                }
              ],
              "name": "gare de Karlsruhe-Hbf",
              "links": [],
              "coord": {
                "lat": "0",
                "lon": "0"
              },
              "label": "gare de Karlsruhe-Hbf",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:80142281"
            }
          },
          "utc_arrival_time": "140200",
          "utc_departure_time": "141200",
          "headsign": "9580",
          "arrival_time": "150200",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71095"
          },
          "departure_time": "151200"
        },
        {
          "stop_point": {
            "name": "gare de Baden-Baden",
            "links": [],
            "coord": {
              "lat": "0",
              "lon": "0"
            },
            "label": "gare de Baden-Baden",
            "equipments": [],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-80142778",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0080-142778-BV"
                },
                {
                  "type": "UIC8",
                  "value": "80142778"
                },
                {
                  "type": "external_code",
                  "value": "OCE80142778"
                }
              ],
              "name": "gare de Baden-Baden",
              "links": [],
              "coord": {
                "lat": "0",
                "lon": "0"
              },
              "label": "gare de Baden-Baden",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:80142778"
            }
          },
          "utc_arrival_time": "143100",
          "utc_departure_time": "143400",
          "headsign": "9580",
          "arrival_time": "153100",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71096"
          },
          "departure_time": "153400"
        },
        {
          "stop_point": {
            "name": "gare de Strasbourg",
            "links": [],
            "coord": {
              "lat": "48.585151",
              "lon": "7.733945"
            },
            "label": "gare de Strasbourg (Strasbourg)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "67482",
                "name": "Strasbourg",
                "level": 8,
                "coord": {
                  "lat": "48.584614",
                  "lon": "7.750712"
                },
                "label": "Strasbourg (67000-67200)",
                "id": "admin:fr:67482",
                "zip_code": "67000;67200"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87212027",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-212027-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87212027"
                },
                {
                  "type": "external_code",
                  "value": "OCE87212027"
                }
              ],
              "name": "gare de Strasbourg",
              "links": [],
              "coord": {
                "lat": "48.585151",
                "lon": "7.733945"
              },
              "label": "gare de Strasbourg (Strasbourg)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87212027"
            }
          },
          "utc_arrival_time": "150300",
          "utc_departure_time": "151200",
          "headsign": "9581",
          "arrival_time": "160300",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71097"
          },
          "departure_time": "161200"
        },
        {
          "stop_point": {
            "name": "gare de Mulhouse",
            "links": [],
            "coord": {
              "lat": "47.741786",
              "lon": "7.342833"
            },
            "label": "gare de Mulhouse (Mulhouse)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "68224",
                "name": "Mulhouse",
                "level": 8,
                "coord": {
                  "lat": "47.749416",
                  "lon": "7.339935"
                },
                "label": "Mulhouse (68100-68200)",
                "id": "admin:fr:68224",
                "zip_code": "68100;68200"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87182063",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-182063-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87182063"
                },
                {
                  "type": "external_code",
                  "value": "OCE87182063"
                }
              ],
              "name": "gare de Mulhouse",
              "links": [],
              "coord": {
                "lat": "47.741786",
                "lon": "7.342833"
              },
              "label": "gare de Mulhouse (Mulhouse)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87182063"
            }
          },
          "utc_arrival_time": "155900",
          "utc_departure_time": "160800",
          "headsign": "9580",
          "arrival_time": "165900",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71098"
          },
          "departure_time": "170800"
        },
        {
          "stop_point": {
            "name": "gare de Belfort-Montbéliard-TGV",
            "links": [],
            "coord": {
              "lat": "47.586579",
              "lon": "6.899019"
            },
            "label": "gare de Belfort-Montbéliard-TGV (Meroux)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "90068",
                "name": "Meroux",
                "level": 8,
                "coord": {
                  "lat": "47.596069",
                  "lon": "6.899145"
                },
                "label": "Meroux (90400)",
                "id": "admin:fr:90068",
                "zip_code": "90400"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87300822",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-300822-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87300822"
                },
                {
                  "type": "external_code",
                  "value": "OCE87300822"
                }
              ],
              "name": "gare de Belfort-Montbéliard-TGV",
              "links": [],
              "coord": {
                "lat": "47.586579",
                "lon": "6.899019"
              },
              "label": "gare de Belfort-Montbéliard-TGV (Meroux)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87300822"
            }
          },
          "utc_arrival_time": "163000",
          "utc_departure_time": "163300",
          "headsign": "9580",
          "arrival_time": "173000",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71099"
          },
          "departure_time": "173300"
        },
        {
          "stop_point": {
            "name": "gare de Besançon-Franche-Comté",
            "links": [],
            "coord": {
              "lat": "47.30746",
              "lon": "5.954751"
            },
            "label": "gare de Besançon-Franche-Comté (Les Auxons)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "25035",
                "name": "Les Auxons",
                "level": 8,
                "coord": {
                  "lat": "47.301167",
                  "lon": "5.957158"
                },
                "label": "Les Auxons (25870)",
                "id": "admin:fr:25035",
                "zip_code": "25870"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87300863",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-300863-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87300863"
                },
                {
                  "type": "external_code",
                  "value": "OCE87300863"
                }
              ],
              "name": "gare de Besançon-Franche-Comté",
              "links": [],
              "coord": {
                "lat": "47.30746",
                "lon": "5.954751"
              },
              "label": "gare de Besançon-Franche-Comté (Les Auxons)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87300863"
            }
          },
          "utc_arrival_time": "165400",
          "utc_departure_time": "165900",
          "headsign": "9580",
          "arrival_time": "175400",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71100"
          },
          "departure_time": "175900"
        },
        {
          "stop_point": {
            "name": "gare de Chalon-sur-Saône",
            "links": [],
            "coord": {
              "lat": "46.781666",
              "lon": "4.84323"
            },
            "label": "gare de Chalon-sur-Saône (Chalon-sur-Saône)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "71076",
                "name": "Chalon-sur-Saône",
                "level": 8,
                "coord": {
                  "lat": "46.788898",
                  "lon": "4.85296"
                },
                "label": "Chalon-sur-Saône (71100)",
                "id": "admin:fr:71076",
                "zip_code": "71100"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87725002",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-725002-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87725002"
                },
                {
                  "type": "external_code",
                  "value": "OCE87725002"
                }
              ],
              "name": "gare de Chalon-sur-Saône",
              "links": [],
              "coord": {
                "lat": "46.781666",
                "lon": "4.84323"
              },
              "label": "gare de Chalon-sur-Saône (Chalon-sur-Saône)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87725002"
            }
          },
          "utc_arrival_time": "175400",
          "utc_departure_time": "175600",
          "headsign": "9581",
          "arrival_time": "185400",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71101"
          },
          "departure_time": "185600"
        },
        {
          "stop_point": {
            "name": "gare de Lyon-Part-Dieu",
            "links": [],
            "coord": {
              "lat": "45.76058",
              "lon": "4.859438"
            },
            "label": "gare de Lyon-Part-Dieu (Lyon)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "69123",
                "name": "Lyon",
                "level": 8,
                "coord": {
                  "lat": "45.757812",
                  "lon": "4.832011"
                },
                "label": "Lyon (69001-69009)",
                "id": "admin:fr:69123",
                "zip_code": "69001;69009"
              },
              {
                "insee": "69383",
                "name": "Lyon 3e Arrondissement",
                "level": 9,
                "coord": {
                  "lat": "45.759933",
                  "lon": "4.849389"
                },
                "label": "Lyon 3e Arrondissement (69003)",
                "id": "admin:fr:69383",
                "zip_code": "69003"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87723197",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-723197-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87723197"
                },
                {
                  "type": "external_code",
                  "value": "OCE87723197"
                }
              ],
              "name": "gare de Lyon-Part-Dieu",
              "links": [],
              "coord": {
                "lat": "45.76058",
                "lon": "4.859438"
              },
              "label": "gare de Lyon-Part-Dieu (Lyon)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87723197"
            }
          },
          "utc_arrival_time": "185600",
          "utc_departure_time": "190600",
          "headsign": "9581",
          "arrival_time": "195600",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71102"
          },
          "departure_time": "200600"
        },
        {
          "stop_point": {
            "name": "gare de Avignon-TGV",
            "links": [],
            "coord": {
              "lat": "43.921963",
              "lon": "4.78616"
            },
            "label": "gare de Avignon-TGV (Avignon)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "84007",
                "name": "Avignon",
                "level": 8,
                "coord": {
                  "lat": "43.949314",
                  "lon": "4.806032"
                },
                "label": "Avignon (84000)",
                "id": "admin:fr:84007",
                "zip_code": "84000"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87318964",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-318964-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87318964"
                },
                {
                  "type": "external_code",
                  "value": "OCE87318964"
                }
              ],
              "name": "gare de Avignon-TGV",
              "links": [],
              "coord": {
                "lat": "43.921963",
                "lon": "4.78616"
              },
              "label": "gare de Avignon-TGV (Avignon)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87318964"
            }
          },
          "utc_arrival_time": "200800",
          "utc_departure_time": "201100",
          "headsign": "9581",
          "arrival_time": "210800",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71103"
          },
          "departure_time": "211100"
        },
        {
          "stop_point": {
            "name": "gare de Aix-en-Provence-TGV",
            "links": [],
            "coord": {
              "lat": "43.455151",
              "lon": "5.317273"
            },
            "label": "gare de Aix-en-Provence-TGV (Aix-en-Provence)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "13001",
                "name": "Aix-en-Provence",
                "level": 8,
                "coord": {
                  "lat": "43.529842",
                  "lon": "5.447473"
                },
                "label": "Aix-en-Provence (13090-13100)",
                "id": "admin:fr:13001",
                "zip_code": "13090;13100"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87319012",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-319012-00"
                },
                {
                  "type": "UIC8",
                  "value": "87319012"
                },
                {
                  "type": "external_code",
                  "value": "OCE87319012"
                }
              ],
              "name": "gare de Aix-en-Provence-TGV",
              "links": [],
              "coord": {
                "lat": "43.455151",
                "lon": "5.317273"
              },
              "label": "gare de Aix-en-Provence-TGV (Aix-en-Provence)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87319012"
            }
          },
          "utc_arrival_time": "203100",
          "utc_departure_time": "203400",
          "headsign": "9581",
          "arrival_time": "213100",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71104"
          },
          "departure_time": "213400"
        },
        {
          "stop_point": {
            "name": "gare de Marseille-St-Charles",
            "links": [],
            "coord": {
              "lat": "43.30273",
              "lon": "5.380659"
            },
            "label": "gare de Marseille-St-Charles (Marseille)",
            "equipments": [],
            "administrative_regions": [
              {
                "insee": "13055",
                "name": "Marseille",
                "level": 8,
                "coord": {
                  "lat": "43.296173",
                  "lon": "5.369952"
                },
                "label": "Marseille (13000-13016)",
                "id": "admin:fr:13055",
                "zip_code": "13000;13016"
              }
            ],
            "fare_zone": {
              "name": "0"
            },
            "id": "stop_point:OCE:SP:TGV-87751008",
            "stop_area": {
              "codes": [
                {
                  "type": "CR-CI-CH",
                  "value": "0087-751008-BV"
                },
                {
                  "type": "UIC8",
                  "value": "87751008"
                },
                {
                  "type": "external_code",
                  "value": "OCE87751008"
                }
              ],
              "name": "gare de Marseille-St-Charles",
              "links": [],
              "coord": {
                "lat": "43.30273",
                "lon": "5.380659"
              },
              "label": "gare de Marseille-St-Charles (Marseille)",
              "timezone": "Europe/Paris",
              "id": "stop_area:OCE:SA:87751008"
            }
          },
          "utc_arrival_time": "204600",
          "utc_departure_time": "204600",
          "headsign": "9581",
          "arrival_time": "214600",
          "journey_pattern_point": {
            "id": "journey_pattern_point:71105"
          },
          "departure_time": "214600"
        }
      ],
      "validity_pattern": {
        "beginning_date": "20120913",
        "days": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011111101111111111111111111111111111111110000000000000000000000000000000000000000000000"
      },
      "id": "vehicle_journey:OCE:SN009580F03012_dst_1",
      "trip": {
        "id": "OCE:SN009580F03012",
        "name": "9580"
      }
    }
  ]
}"""
