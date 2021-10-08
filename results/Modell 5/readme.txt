Sechster Job

- verwendet als Quellen nur Pressemitteilungen der INSM mit len() > 2500 () -> 152 Texte
- berücksichtigt nur Zieltexte mit len() > 2500, ohne ["sport", "ratgeber", "unterhaltung", "lifestyle", "leute"] in der URL (effektiv Kategoriefilter), ohne Verweise auf Abo/weiterlesen im Text -> 49337 Texte
- neues Modell, das auf METER-Daten und zusätzlichen Texten aus dem BNC trainiert wurde. Enthält 3324 zusätzliche "nd" Texte, Typ RandomForestClassifier
