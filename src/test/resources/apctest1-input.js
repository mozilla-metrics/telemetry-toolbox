{ 
    "slowSQL": {
        "mainThread": {
            "SELECT a.item_id FROM moz_anno_attributes n JOIN moz_items_annos a ON n.id = a.anno_attribute_id WHERE n.name = :anno_name": [1, 122],
            "INSERT INTO locale (name, description, creator, homepageURL) VALUES (:name, :description, :creator, :homepageURL)": [1, 146]
        },
        "otherThreads": {
            "SELECT * FROM moz_places WHERE url LIKE '%facebook.com/blah%' ORDER BY frecency DESC LIMIT 3": [5, 1000],
            "SELECT * FROM moz_places WHERE url LIKE '%twitter.com/blah%' ORDER BY frecency DESC LIMIT 3": [5, 1000],
            "SELECT * FROM moz_places WHERE domain IN ('twitter.com','facebook.com')": [5, 1000],
            "SELECT * FROM moz_places WHERE domain IN (\"twitter.com\",\"facebook.com\")": [5, 1000],
            "SELECT * FROM moz_places WHERE domain NOT IN('twitter.com','facebook.com')": [5, 1000]
        }
    }
}

