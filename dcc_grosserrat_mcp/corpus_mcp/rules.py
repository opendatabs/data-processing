"""What the open data states explicitly, as queries for nodes and edges."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NodeRule:
    """A query that yields nodes."""

    name: str
    sql: str


@dataclass(frozen=True, slots=True)
class EdgeRule:
    """A query that yields triples."""

    predicate: str
    sql: str
    source: str = "ogd"


# Business item of an agenda signature: '21.5014.02' → '21.5014'.
BUSINESS_ITEM_OF = """CASE
    WHEN instr(substr(j.value, instr(j.value, '.') + 1), '.') > 0
    THEN substr(j.value, 1, instr(j.value, '.') + instr(substr(j.value, instr(j.value, '.') + 1), '.') - 1)
    ELSE j.value END"""

NODE_RULES: tuple[NodeRule, ...] = (
    NodeRule(
        "business_items",
        """
        SELECT 'ges:' || signatur_ges AS id, 'geschaeft' AS type,
               COALESCE(NULLIF(titel_ges, ''), signatur_ges) AS name, url_ges AS url,
               json_object('art', ga_rr_gr, 'status', status_ges, 'beginn', beginn_ges,
                           'ende', ende_ges, 'departement', departement_ges) AS attrs_json
        FROM geschaefte WHERE signatur_ges != ''
        """,
    ),
    NodeRule(
        "business_items_from_agendas",
        f"""
        SELECT DISTINCT 'ges:' || {BUSINESS_ITEM_OF} AS id, 'geschaeft' AS type,
               MIN(t.titel) AS name, NULL AS url, json_object('quelle', 'tagesordnung') AS attrs_json
        FROM tagesordnungen t, json_each(t.signatur_ges) j
        WHERE t.signatur_ges != '' AND j.value != ''
        GROUP BY 1
        """,
    ),
    NodeRule(
        "people_mentioned",
        """
        SELECT 'person:' || nr AS id, 'person' AS type, name, NULL AS url,
               json_object('quelle', 'erwaehnung') AS attrs_json
        FROM (
            SELECT urheber_nr AS nr, urheber AS name FROM geschaefte
            WHERE urheber_nr != '' AND urheber != '' AND gremientyp_urheber = ''
            UNION SELECT nr, name_vorname FROM miturheber WHERE nr != '' AND name_vorname != ''
            UNION SELECT uni_nr, name_vorname FROM interessen WHERE uni_nr != '' AND name_vorname != ''
        )
        """,
    ),
    NodeRule(
        "people",
        """
        SELECT 'person:' || uni_nr AS id, 'person' AS type, name_vorname AS name,
               -- The member page resolves by number alone; the name part of the URL is
               -- decoration. '?mnr=' redirects twice instead.
               'https://grosserrat.bs.ch/mitglieder/' || uni_nr AS url,
               json_object('partei', partei, 'wahlkreis', wahlkreis, 'beruf', beruf,
                           'gr_beginn', gr_beginn, 'gr_ende', gr_ende,
                           'ist_aktuell', ist_aktuell, 'amtszeiten', json(amtszeiten)) AS attrs_json
        -- One row per term in the open data; the entry describes the current term, else the
        -- latest, and lists every term.
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY uni_nr ORDER BY ist_aktuell = 'Ja' DESC, gr_beginn DESC
                   ) AS position,
                   (SELECT json_group_array(json_object('beginn', t.gr_beginn, 'ende', t.gr_ende))
                    FROM (SELECT gr_beginn, gr_ende FROM personen t WHERE t.uni_nr = p.uni_nr
                          ORDER BY gr_beginn) t) AS amtszeiten
            FROM personen p WHERE uni_nr != '' AND name_vorname != ''
        ) WHERE position = 1
        """,
    ),
    NodeRule(
        "committees_mentioned",
        """
        SELECT 'gremium:' || nr AS id, 'gremium' AS type, name, NULL AS url,
               json_object('quelle', 'erwaehnung') AS attrs_json
        FROM (
            SELECT uni_nr_an AS nr, name_an AS name FROM zuweisungen WHERE uni_nr_an != '' AND name_an != ''
            UNION SELECT uni_nr_gre, name_gre FROM mitgliedschaften WHERE uni_nr_gre != '' AND name_gre != ''
            UNION SELECT urheber_nr, urheber FROM geschaefte WHERE urheber_nr != '' AND gremientyp_urheber != ''
        )
        """,
    ),
    NodeRule(
        "committees",
        """
        SELECT 'gremium:' || uni_nr AS id, 'gremium' AS type,
               COALESCE(NULLIF(name, ''), kurzname) AS name, NULL AS url,
               json_object('kurzname', kurzname, 'typ', gremientyp) AS attrs_json
        FROM gremien WHERE uni_nr != ''
        """,
    ),
    NodeRule(
        "parties",
        """
        SELECT DISTINCT 'partei:' || p AS id, 'partei' AS type, p AS name, NULL AS url, NULL AS attrs_json
        FROM (
            SELECT partei_urheber AS p FROM geschaefte
            UNION SELECT partei FROM miturheber
            UNION SELECT partei FROM personen
            UNION SELECT fraktion FROM stimmen
        ) WHERE p != ''
        """,
    ),
    NodeRule(
        "organisations",
        """
        SELECT DISTINCT 'org:' || lower(replace(organisation, ' ', '_')) AS id, 'organisation' AS type,
               organisation AS name, NULL AS url, NULL AS attrs_json
        FROM interessen WHERE organisation != ''
        """,
    ),
    NodeRule(
        "topics",
        """
        SELECT DISTINCT 'thema:' || lower(replace(t, ' ', '_')) AS id, 'thema' AS type,
               t AS name, NULL AS url, NULL AS attrs_json
        FROM (
            SELECT thema_1 AS t FROM vorstoesse
            UNION SELECT thema_2 FROM vorstoesse
            UNION SELECT schwerpunkt FROM vorstoesse
        ) WHERE t != ''
        """,
    ),
    NodeRule(
        "sessions",
        """
        SELECT DISTINCT 'sitzung:' || tagesordnung_idnr AS id, 'sitzung' AS type,
               COALESCE(NULLIF(text1, ''), 'Sitzung') || ' vom ' || tag1 AS name,
               url_vollprotokoll AS url, json_object('tag', tag1) AS attrs_json
        FROM tagesordnungen WHERE tagesordnung_idnr != '' AND tag1 != ''
        """,
    ),
    NodeRule(
        "departments",
        """
        SELECT DISTINCT 'dep:' || j.value AS id, 'departement' AS type, j.value AS name,
               NULL AS url, NULL AS attrs_json
        FROM tagesordnungen t, json_each(t.departement) j
        WHERE t.departement != '' AND j.value != ''
        """,
    ),
)

EDGE_RULES: tuple[EdgeRule, ...] = (
    EdgeRule(
        "AUTHORED_BY",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'person:' || urheber_nr AS object_id,
               'Urheber' AS qualifier, beginn_ges AS valid_from, NULL AS valid_to, NULL AS weight
        FROM geschaefte WHERE urheber_nr != '' AND gremientyp_urheber = ''
        """,
    ),
    EdgeRule(
        "AUTHORED_BY",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'gremium:' || urheber_nr AS object_id,
               gremientyp_urheber AS qualifier, beginn_ges AS valid_from, NULL AS valid_to, NULL AS weight
        FROM geschaefte WHERE urheber_nr != '' AND gremientyp_urheber != ''
        """,
    ),
    EdgeRule(
        "CO_SPONSORED_BY",
        """
        SELECT 'ges:' || m.signatur_ges AS subject_id, 'person:' || m.nr AS object_id,
               'Miturheber' AS qualifier, g.beginn_ges AS valid_from, NULL AS valid_to, NULL AS weight
        FROM miturheber m LEFT JOIN geschaefte g ON g.signatur_ges = m.signatur_ges
        WHERE m.nr != ''
        """,
    ),
    EdgeRule(
        "SUBMITTED_BY_PARTY",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'partei:' || partei_urheber AS object_id,
               '' AS qualifier, beginn_ges AS valid_from, NULL AS valid_to, NULL AS weight
        FROM geschaefte WHERE partei_urheber != ''
        """,
    ),
    EdgeRule(
        "REFERRED_TO",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'gremium:' || uni_nr_an AS object_id,
               titel_zuw AS qualifier, NULL AS valid_from, erledigt AS valid_to, NULL AS weight
        FROM zuweisungen WHERE signatur_ges != '' AND uni_nr_an != ''
        """,
    ),
    EdgeRule(
        "MEMBER_OF",
        """
        SELECT 'person:' || uni_nr_adr AS subject_id, 'gremium:' || uni_nr_gre AS object_id,
               funktion AS qualifier, beginn AS valid_from, ende AS valid_to, NULL AS weight
        FROM mitgliedschaften WHERE uni_nr_adr != '' AND uni_nr_gre != ''
        """,
    ),
    EdgeRule(
        "BELONGS_TO_PARTY",
        """
        SELECT 'person:' || uni_nr AS subject_id, 'partei:' || partei AS object_id,
               '' AS qualifier, gr_beginn AS valid_from, gr_ende AS valid_to, NULL AS weight
        FROM personen WHERE uni_nr != '' AND partei != ''
        """,
    ),
    EdgeRule(
        "HAS_INTEREST",
        """
        SELECT 'person:' || uni_nr AS subject_id,
               'org:' || lower(replace(organisation, ' ', '_')) AS object_id,
               funktion AS qualifier, NULL AS valid_from, NULL AS valid_to, NULL AS weight
        FROM interessen WHERE uni_nr != '' AND organisation != ''
        """,
    ),
    EdgeRule(
        "HAS_TOPIC",
        """
        SELECT 'ges:' || signatur AS subject_id, 'thema:' || lower(replace(t, ' ', '_')) AS object_id,
               r AS qualifier, beginn_datum AS valid_from, NULL AS valid_to, NULL AS weight
        FROM (
            SELECT signatur, beginn_datum, thema_1 AS t, 'thema_1' AS r FROM vorstoesse
            UNION ALL SELECT signatur, beginn_datum, thema_2, 'thema_2' FROM vorstoesse
            UNION ALL SELECT signatur, beginn_datum, schwerpunkt, 'schwerpunkt' FROM vorstoesse
        ) WHERE t != ''
        """,
    ),
    EdgeRule(
        "VOTED",
        """
        SELECT 'person:' || gr_uni_nr AS subject_id, 'ges:' || signatur_ges AS object_id,
               entscheid AS qualifier, MIN(datum) AS valid_from, MAX(datum) AS valid_to,
               CAST(COUNT(*) AS REAL) AS weight
        FROM stimmen WHERE gr_uni_nr != '' AND signatur_ges != '' AND entscheid IN ('J', 'N')
        GROUP BY 1, 2, 3
        """,
    ),
    EdgeRule(
        "SUPPORTED_BY_PARTY",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'partei:' || fraktion AS object_id,
               'J' AS qualifier, MIN(datum) AS valid_from, MAX(datum) AS valid_to,
               CAST(COUNT(*) AS REAL) AS weight
        FROM stimmen WHERE entscheid = 'J' AND signatur_ges != '' AND fraktion != ''
        GROUP BY 1, 2
        """,
    ),
    EdgeRule(
        "OPPOSED_BY_PARTY",
        """
        SELECT 'ges:' || signatur_ges AS subject_id, 'partei:' || fraktion AS object_id,
               'N' AS qualifier, MIN(datum) AS valid_from, MAX(datum) AS valid_to,
               CAST(COUNT(*) AS REAL) AS weight
        FROM stimmen WHERE entscheid = 'N' AND signatur_ges != '' AND fraktion != ''
        GROUP BY 1, 2
        """,
    ),
    EdgeRule(
        "SCHEDULED_ON",
        f"""
        SELECT 'ges:' || {BUSINESS_ITEM_OF} AS subject_id,
               'sitzung:' || t.tagesordnung_idnr AS object_id,
               t.status AS qualifier, t.tag1 AS valid_from, NULL AS valid_to, NULL AS weight
        FROM tagesordnungen t, json_each(t.signatur_ges) j
        WHERE t.signatur_ges != '' AND t.tagesordnung_idnr != '' AND j.value != ''
        """,
    ),
    EdgeRule(
        "HANDLED_BY_DEPARTEMENT",
        f"""
        SELECT 'ges:' || {BUSINESS_ITEM_OF} AS subject_id, 'dep:' || d.value AS object_id,
               '' AS qualifier, MIN(t.tag1) AS valid_from, NULL AS valid_to, NULL AS weight
        FROM tagesordnungen t, json_each(t.signatur_ges) j, json_each(t.departement) d
        WHERE t.signatur_ges != '' AND t.departement != '' AND j.value != '' AND d.value != ''
        GROUP BY 1, 2
        """,
    ),
    EdgeRule(
        "REFERRED_TO",
        f"""
        SELECT 'ges:' || {BUSINESS_ITEM_OF} AS subject_id, 'gremium:' || gr.uni_nr AS object_id,
               'Traktandum' AS qualifier, MIN(t.tag1) AS valid_from, NULL AS valid_to, NULL AS weight
        FROM tagesordnungen t, json_each(t.signatur_ges) j, json_each(t.kommission) k
        JOIN gremien gr ON gr.kurzname = k.value
        WHERE t.signatur_ges != '' AND t.kommission != '' AND j.value != ''
        GROUP BY 1, 2
        """,
    ),
)
