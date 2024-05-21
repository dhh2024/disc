from collections import OrderedDict
from typing import Any
import click
from mariadb import Cursor
from more_itertools import chunked
import stanza
from tqdm.auto import tqdm
import pandas as pd
from hereutil import here, add_to_sys_path
from stanza.models.common.utils import space_before_to_misc, space_after_to_misc
add_to_sys_path(here())
from src.common_basis import *  # noqa

stanza.download('en')
nlp = stanza.Pipeline(
    'en', processors='tokenize,mwt,pos,lemma,depparse,constituency,ner,sentiment')


def create_global_tables(cur: RecoveringCursor):
    cur.execute("""
CREATE TABLE IF NOT EXISTS words_a (
    word_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    word VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (word),
    UNIQUE INDEX word_id (word_id, word)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS lemmas_a (
    lemma_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    lemma VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (lemma),
    UNIQUE INDEX lemma_id (lemma_id, lemma)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS upos_a (
    upos_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    upos CHAR(5) NOT NULL,
    PRIMARY KEY (upos),
    UNIQUE INDEX upos_id (upos_id, upos)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")
    cur.execute(f"""
INSERT IGNORE INTO upos_a (upos) VALUES ("{'"), ("'.join(nlp.processors['pos'].get_known_upos())}")
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS xpos_a (
    xpos_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    xpos VARCHAR(255) NOT NULL,
    PRIMARY KEY (xpos),
    UNIQUE INDEX xpos_id (xpos_id, xpos)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")
    cur.execute(f"""
INSERT IGNORE INTO xpos_a (xpos) VALUES ("{'"), ("'.join(nlp.processors['pos'].get_known_xpos())}")
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS feats_a (
    feats_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    feats VARCHAR(255) NOT NULL,
    PRIMARY KEY (feats),
    UNIQUE INDEX feats_id (feats_id, feats)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute("""
CREATE TABLE IF NOT EXISTS deprel_a (
    deprel_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    deprel VARCHAR(255) NOT NULL,
    PRIMARY KEY (deprel),
    UNIQUE INDEX deprel_id (deprel_id, deprel)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute(f"""
INSERT IGNORE INTO deprel_a (deprel) VALUES ("{'"), ("'.join(nlp.processors['depparse'].get_known_relations())}")
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS misc_a (
    misc_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
    misc VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (misc),
    UNIQUE INDEX misc_id (misc_id, misc)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")


def create_tables(cur: RecoveringCursor, table: str):
    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {table}_parse_a (
    text_id BIGINT UNSIGNED NOT NULL, -- unique numeric id of the submission or comment parsed
    sentence_id SMALLINT UNSIGNED NOT NULL, -- sentence number in the submission or comment
    word_pos SMALLINT UNSIGNED NOT NULL, -- word position in the sentence
    word_id INT UNSIGNED NOT NULL, -- word_id that references words_a which contains the actual text of the word
    lemma_id INT UNSIGNED NOT NULL, -- lemma_id that references lemmas_a which contains the actual text of the lemma
    upos_id TINYINT UNSIGNED NOT NULL, -- upos_id that references upos_a which contains the actual upos
    xpos_id TINYINT UNSIGNED NOT NULL, -- xpos_id that references upos_a which contains the actual xpos
    feats_id SMALLINT UNSIGNED, -- feats_id that references feats_a which contains the actual feats
    head_pos SMALLINT UNSIGNED NOT NULL, -- the word position of the head of the dependency relation
    deprel_id TINYINT UNSIGNED NOT NULL, -- deprel_id that references deprel_a which contains the actual dependency relation
    misc_id SMALLINT UNSIGNED, -- misc_id that references misc_a which contains the actual misc text associated with the word
    start_char SMALLINT UNSIGNED NOT NULL, -- start character position of the word in the submission/comment
    end_char SMALLINT UNSIGNED NOT NULL, -- end character position of the word in the submission/comment
    PRIMARY KEY (text_id, sentence_id, word_pos)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {table}_parse_sentiments_a (
    text_id BIGINT UNSIGNED NOT NULL, -- unique numeric id of the submission or comment parsed
    sentence_id SMALLINT UNSIGNED NOT NULL, -- sentence number in the submission or comment
    sentiment TINYINT NOT NULL, -- sentiment score of the sentence
    PRIMARY KEY (text_id, sentence_id)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {table}_parse_constituents_a (
    text_id BIGINT UNSIGNED NOT NULL, -- unique numeric id of the submission or comment parsed
    sentence_id SMALLINT UNSIGNED NOT NULL, -- sentence number in the submission or comment
    node_id SMALLINT UNSIGNED NOT NULL, -- node id of the constituent within the tree (root is 0)
    label_id INT UNSIGNED NOT NULL, -- references words_a which contains the actual text of the label in the tree
    parent_node_id SMALLINT UNSIGNED NOT NULL, -- parent node id of the constituent within the tree
    PRIMARY KEY (text_id, sentence_id, node_id)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")

    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {table}_parse_entities_a (
    name VARCHAR(255) NOT NULL, -- the name of the entity
    text_id BIGINT UNSIGNED NOT NULL, -- unique numeric id of the submission or comment parsed
    sentence_id SMALLINT UNSIGNED NOT NULL, -- sentence number in the submission or comment
    start_word_pos INT UNSIGNED NOT NULL, -- word position in the sentence where the entity mention starts
    end_word_pos INT UNSIGNED NOT NULL, -- word position in the sentence where the entity mention ends
    PRIMARY KEY (name, text_id, sentence_id, start_word_id, end_word_id),
    INDEX (text_id, sentence_id)
) ENGINE=ARIA CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci TRANSACTIONAL=0 PAGE_CHECKSUM=0
""")


def add_unknown_constituents(node: Any, word_cache: LRUCache[str, int], unknown_words: set[str]) -> None:
    if node.label not in word_cache._items:
        unknown_words.add(node.label)
    for child in node.children:
        add_unknown_constituents(child, word_cache, unknown_words)


def process_constituents(node: Any, parent_node_id: int, word_cache: LRUCache[str, int], constituents: list[tuple[int, int, int]]) -> None:
    node_id = len(constituents) + 1
    constituents.append(
        (node_id, word_cache[node.label], parent_node_id))  # type: ignore
    for child in node.children:
        process_constituents(child, node_id, word_cache, constituents)


@click.option('-t', '--table', required=True, help="table to parse")
@click.command
def parse_dataset(table: str):
    rcur = get_recovering_cursor()
    mcur = get_recovering_cursor()
    create_global_tables(mcur)
    create_tables(mcur, table)
    n: int = rcur.execute(f"SELECT COUNT(*) FROM {table}_a").fetchall()[0][0]
    pbar = tqdm(total=n)
    lemma_cache = LRUCache[str, int](max_size=65536)
    word_cache = LRUCache[str, int](max_size=65536)
    upos = dict()
    for row in rcur.execute("SELECT upos, upos_id FROM upos_a").fetchall():
        upos[row[0]] = row[1]
    xpos = dict()
    for row in rcur.execute("SELECT xpos, xpos_id FROM xpos_a").fetchall():
        xpos[row[0]] = row[1]
    feats = dict()
    for row in rcur.execute("SELECT feats, feats_id FROM feats_a").fetchall():
        feats[row[0]] = row[1]
    misc = dict()
    for row in rcur.execute("SELECT misc, misc_id FROM misc_a").fetchall():
        misc[row[0]] = row[1]
    deprel = dict()
    for row in rcur.execute("SELECT deprel, deprel_id FROM deprel_a").fetchall():
        deprel[row[0]] = row[1]
    rcur.execute(f"SELECT id, {"body" if "comments" in table else "selftext AS body"} FROM {table}_a",
                 buffered=False)
    while True:
        results = rcur.fetchmany(1000)
        if not results:
            break
        r = nlp.bulk_process([row[1] for row in results])
        unknown_lemmas = set()
        unknown_words = set()
        unknown_feats = set()
        unknown_misc = set()
        for doc in r:
            for sentence in doc.sentences:
                for token in sentence.tokens:
                    sbmisc = space_before_to_misc(token.spaces_before)
                    if sbmisc != '':
                        if token.words[0].misc:
                            token.words[0].misc = token.words[0].misc + \
                                '|' + sbmisc
                        else:
                            token.words[0].misc = sbmisc
                    samisc = space_after_to_misc(token.spaces_after)
                    if samisc != '':
                        if token.words[-1].misc:
                            token.words[-1].misc = token.words[-1].misc + \
                                '|' + samisc
                        else:
                            token.words[-1].misc = samisc
                    for word in token.words:
                        if word.lemma not in lemma_cache._items:
                            unknown_lemmas.add(word.lemma)
                        if word.text not in word_cache._items:
                            unknown_words.add(word.text)
                        if word.feats is not None and word.feats not in feats:
                            unknown_feats.add(word.feats)
                        if word.misc is not None and word.misc not in misc:
                            unknown_misc.add(word.misc)
                add_unknown_constituents(
                    sentence.constituency, word_cache, unknown_words)
        if unknown_lemmas:
            mcur.executemany(
                f"INSERT IGNORE INTO lemmas_a (lemma) VALUES (?)", [(lemma,) for lemma in unknown_lemmas])
            for chunk in chunked(unknown_lemmas, 1000):
                for row in mcur.execute(f"SELECT * FROM lemmas_a WHERE lemma IN ({', '.join(('?' for _ in chunk))})", chunk).fetchall():
                    lemma_cache[row[1]] = row[0]
        if unknown_words:
            mcur.executemany(
                f"INSERT IGNORE INTO words_a (word) VALUES (?)", [(word,) for word in unknown_words])
            for chunk in chunked(unknown_words, 1000):
                for row in mcur.execute(f"SELECT * FROM words_a WHERE word IN ({', '.join(('?' for _ in chunk))})", chunk).fetchall():
                    word_cache[row[1]] = row[0]
        if unknown_feats:
            mcur.executemany(
                f"INSERT IGNORE INTO feats_a (feats) VALUES (?)", [(feats,) for feats in unknown_feats])
            for chunk in chunked(unknown_feats, 1000):
                for row in mcur.execute(f"SELECT * FROM feats_a WHERE feats IN ({', '.join(('?' for _ in chunk))})", chunk).fetchall():
                    feats[row[1]] = row[0]
        if unknown_misc:
            mcur.executemany(
                f"INSERT IGNORE INTO misc_a (misc) VALUES (?)", [(misc,) for misc in unknown_misc])
            for chunk in chunked(unknown_misc, 1000):
                for row in mcur.execute(f"SELECT * FROM misc_a WHERE misc IN ({', '.join(('?' for _ in chunk))})", chunk).fetchall():
                    misc[row[1]] = row[0]
        ent_stmts = []
        stmts = []
        for text_id, doc in zip((row[0] for row in results), r):
            for sent_id, sentence in enumerate(doc.sentences):
                if sentence.sentiment:
                    mcur.execute(f"INSERT IGNORE INTO {table}_parse_sentiments_a VALUES (?, ?, ?)",
                                 (text_id, sent_id + 1, sentence.sentiment))
                for entity in sentence.entities:
                    ent_stmts.append(
                        (entity.text, text_id, sent_id + 1, entity.words[0].id, entity.words[-1].id))
                constituents: list[tuple[int, int, int]] = []
                process_constituents(sentence.constituency,
                                     0, word_cache, constituents)
                mcur.executemany(f"INSERT IGNORE INTO {table}_parse_constituents_a VALUES (?, ?, ?, ?, ?)",
                                 [(text_id, sent_id + 1, node_id, label_id, parent_node_id) for node_id, label_id, parent_node_id in constituents])
                for word in sentence.words:
                    stmts.append((
                        text_id,
                        sent_id + 1,
                        word.id,
                        word_cache[word.text],
                        lemma_cache[word.lemma],
                        upos[word.upos],
                        xpos[word.xpos],
                        feats.get(
                            word.feats) if word.feats is not None else None,
                        word.head,
                        deprel[word.deprel],
                        misc.get(word.misc) if word.misc is not None else None,
                        word.start_char,
                        word.end_char))
        lemma_cache._trim_to_size()
        word_cache._trim_to_size()
        mcur.executemany(f"INSERT IGNORE INTO {table}_parse_entities_a VALUES (?, ?, ?, ?, ? )",
                         ent_stmts)
        mcur.executemany(f"INSERT IGNORE INTO {table}_parse_a VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         stmts)
        pbar.update(len(results))


if __name__ == '__main__':
    parse_dataset()
