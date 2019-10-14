import psycopg2
import json


def connect():
    print('Connecting to PostGRESQL db...')
    conn = psycopg2.connect(host="35.243.220.243", database="proj1part2", user="lmb2289", password="8497")
    cur = conn.cursor()

    # print('PostgreSQL database version:')
    # cur.execute('SELECT version()')

    # # display the PostgreSQL database server version
    # db_version = cur.fetchone()
    # print(db_version)
    #
    #
    # print('Testing SELECT * From test')
    # cur.execute("SELECT * FROM test")
    # rows = cur.fetchall()
    # for row in rows:
    #     print(row)
    #
    # closeConnection(conn)

    return conn


def closeConnection(conn):
    conn.cursor().close()


def insertTraitsFromJSON(conn):
    with open('data/5e-SRD-Traits.json') as f:
        traits_dict = json.load(f)

    trait_insert = "INSERT INTO traits(tname,effect) VALUES(%s,%s)"
    trait_race_insert = "INSERT INTO Traits_Provided_By_Race (tname,rname) VALUES (%s,%s)"

    cur = conn.cursor()

    for dnd_trait in traits_dict:

        record_to_insert = (dnd_trait["name"], dnd_trait["desc"])

        cur.execute(trait_insert, record_to_insert)
        conn.commit()

        for dnd_race in dnd_trait["races"]:
            record_feature_class_insert = (dnd_trait["name"], dnd_race["name"])
            cur.execute(trait_race_insert, record_feature_class_insert)
            conn.commit()


def insertRacesFromJSON(conn):
    with open('data/5e-SRD-Races.json') as f:
        races_dict = json.load(f)

    race_insert = "INSERT INTO races(rname,speed) VALUES(%s,%s)"
    proficiency_race_insert = "INSERT INTO Race_Provided_Proficiencies (rname,pname) VALUES (%s,%s)"

    cur = conn.cursor()

    for dnd_race in races_dict:

        record_to_insert = (dnd_race["name"], dnd_race["speed"])

        cur.execute(race_insert, record_to_insert)
        conn.commit()

        for dnd_subrace in dnd_race["subraces"]:
            record_to_insert = (dnd_subrace["name"], dnd_race["speed"])
            cur.execute(race_insert, record_to_insert)
            conn.commit()

        for dnd_proficiency in dnd_race["starting_proficiencies"]:
            record_feature_class_insert = (dnd_race["name"], dnd_proficiency["name"])
            cur.execute(proficiency_race_insert, record_feature_class_insert)
            conn.commit()

def insertProficienciesFromJSON(conn):
    with open('data/5e-SRD-Proficiencies.json') as f:
        features_dict = json.load(f)

    proficiency_insert = "INSERT INTO proficiencies(pname,type) VALUES(%s,%s)"
    proficiency_class_insert = "INSERT INTO Classes_Provided_Proficiencies (cname,pname) VALUES (%s,%s)"
    # proficiency_race_insert = "INSERT INTO Classes_Provided_Proficiencies (cname,pname) VALUES (%s,%s)"

    cur = conn.cursor()

    for dnd_proficiency in features_dict:

        record_to_insert = (dnd_proficiency["name"], dnd_proficiency["type"])

        cur.execute(proficiency_insert, record_to_insert)
        conn.commit()

        for dnd_class in dnd_proficiency["classes"]:
            record_feature_class_insert = (dnd_class["name"], dnd_proficiency["name"])
            cur.execute(proficiency_class_insert, record_feature_class_insert)
            conn.commit()


def insertFeaturesFromJSON(conn):
    with open('data/5e-SRD-Features.json') as f:
        features_dict = json.load(f)

    feature_insert = "INSERT INTO features(fname,description,level,cname) VALUES(%s,%s,%s,%s)"
    feature_class_insert = "INSERT INTO features_learned_by_class (fname,cname,level) VALUES (%s,%s,%s)"
    cur = conn.cursor()

    for dnd_feature in features_dict:

        dnd_feature.get("higher_level", "n/a")

        record_to_insert = (dnd_feature["name"],dnd_feature["desc"],dnd_feature.get("level",'1'),dnd_feature["class"]["name"])

        cur.execute(feature_insert, record_to_insert)
        conn.commit()

        record_feature_class_insert = (dnd_feature["name"], dnd_feature["class"]["name"], dnd_feature.get("level",'1'))

        cur.execute(feature_class_insert, record_feature_class_insert)
        conn.commit()


def insertSpellsFromJSON(conn):
    with open('data/5e-SRD-Spells.json') as f:
        classes_dict = json.load(f)

    spell_insert = "INSERT INTO spells(sname,spell_level,casttime,range,duration,components,highlevel,description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
    spell_class_insert = "INSERT INTO spells_learnable_by_class (sname,cname) VALUES (%s,%s)"
    cur = conn.cursor()

    for dnd_spell in classes_dict:
        componentString = ""
        for component in dnd_spell["components"]:
            componentString += component

        dnd_spell.get("higher_level", "n/a")

        record_to_insert = (
        dnd_spell["name"], dnd_spell["level"], dnd_spell["casting_time"], dnd_spell["range"], dnd_spell["duration"], componentString, dnd_spell.get("higher_level", "n/a"), dnd_spell["desc"])

        cur.execute(spell_insert, record_to_insert)
        conn.commit()

        for usable_class in dnd_spell["classes"]:
            relation_to_insert = (dnd_spell["name"], usable_class["name"])
            cur.execute(spell_class_insert, relation_to_insert)
            conn.commit()


def insertClassesFromJSON(conn):
    with open('data/5e-SRD-Classes.json') as f:
        classes_dict = json.load(f)
    sql = "INSERT INTO classes(hit_die,cname) VALUES(%s,%s)"

    cur = conn.cursor()
    for dnd_class in classes_dict:
        # print(dnd_class['name'])
        # print(dnd_class['hit_die'])
        print("\'" + str(dnd_class["hit_die"]) + "', " "'" + dnd_class["name"] + "\'")
        record_to_insert = ("d" + str(dnd_class["hit_die"]), dnd_class["name"])
        cur.execute(sql, record_to_insert)
        conn.commit()


if __name__ == '__main__':
    conn = connect()
    # insertClassesFromJSON(conn)
    # insertSpellsFromJSON(conn)
    # insertFeaturesFromJSON(conn)
    # insertProficienciesFromJSON(conn)
    # insertRacesFromJSON(conn)
    # insertTraitsFromJSON(conn)
    conn.close()
    print('done!')
