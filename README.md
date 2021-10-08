# insm-reuse
Experimente zum automatischen identifizieren der Wiederverwendung von Material der INSM in der deutschen Presse.



### Funktion der einzelnen Skripte

| Dateiname                     | Funktion                                                     |
| ----------------------------- | ------------------------------------------------------------ |
| ground_truth_similarity.ipynb | Wendet Textmetrikenmodule auf die Ground-Truth-Daten an und speichert diese zusätzlich in der Datenbank |
| load_bnc.ipynb                | Lädt zusätzliche Texte aus dem BNC und speichert diese in die Datenbank als zusätzliche Ground Truth für das augMEnt-Korpus |
| load_meter.py                 | Lädt und verarbeitet die TEI-Version des METER-Korpus und speichert dieses in einer Datenbank |
| load_own.py                   | Lädt und verarbeitet die eigenen Texte und speichert diese in einer Datenbank |
| meter_statistics.ipynb        | Visualisierungen zur statistischen Verteilung der Textmetriken auf der METER-Daten |
| meter_train.ipynb             | Training von Classifiern                                     |
| modified_ngram_similarity.py  | Textmetrikmodul für das Modified-Ngram-Overlap nach Nawab et al. |
| ngram_similarity.py           | Textmetrikmodul für das ursprüngliche Ngram-Overlap nach Clough et al. |
| own_infer.py                  | Skript für das parallelisierte Anwenden der Classifier auf eigene Daten |
| textmetrics.ipynb             | Visualisierungen und Statistiken zu den eigenen gescrapeten Texten |
| join.ipynb                    | Skript zum Zusammenführen mehrerer Ergebnisdatenbanken       |
| prettify.ipynb                | Skript zum extrahieren von positiven Matches aus der Ergebnisdatenbank |

