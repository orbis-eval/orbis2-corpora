#!/usr/bin/env python3

"""
Conver the source dataset to the JSON annotation format.
"""

from zstd import loads
from csv import reader
from pathlib import Path

from orbis2.model.annotation import Annotation
from orbis2.model.annotation_type import AnnotationType
from orbis2.model.annotator import Annotator
from orbis2.model.document import Document
from orbis2.model.metadata import Metadata
from orbis2.model.corpus import Corpus
from orbis2.model.role import Role
from orbis2.model.run import Run

from orbis2.corpus_export.nif import NifExportFormat

ANNOTATOR = Annotator(name='CorpusImporter', roles=[Role(name='CorpusImporter')])
CORPUS_URL = "https://github.com/orbis-eval/orbis2-corpora/groningen-meaning-bank/abhinavwalia95-ner/{}"
ANNOTATION_TYPE_URLS = {
    'per': 'http://dbpedia.org/ontology/Person',
    'org': 'http://dbpedia.org/ontology/Organisation',
    'geo': 'http://dbpedia.org/ontology/Place',
    'tim': 'http://www.w3.org/2006/time#Instant',
    'gpe': 'http://schema.org/AdministrativeArea',
    'art': 'http://github.com/orbis-eval/orbis2-corpora/groningen-meaning-bank/class#art',
    'eve': 'http://dbpedia.org/ontology/Event',
    'nat': 'http://dbpedia.org/ontology/NaturalPhenomenon',
}


class CurrentAnnotation:

    def __init__(self):
        self.start = 0
        self.end = 0
        self.annotation_type = None
        self.supported_annotation_types = set()
        self.key = CORPUS_URL.format(1)

    def set_sentence_number(self, no):
        self.key = CORPUS_URL.format(no)


    def yield_annotation(self, sentence):
        self.supported_annotation_types.add(self.annotation_type)
        surface_form = sentence[self.start: self.end]
        if surface_form:
            return {'annotation_type': self.annotation_type,
                    'key': self.key + f"#{self.start},{self.end}",
                    'start': self.start,
                    'end': self.end,
                    'surface_form': surface_form}
        return None

    def register_word(self, start, end, annotation_type, sentence):
        result = []
        if annotation_type.startswith("B-"):
            if self.annotation_type:
                result.append(self.yield_annotation(sentence))
            self.annotation_type = ANNOTATION_TYPE_URLS[annotation_type.split("-")[1]]
            self.start = start
            self.end = end
        elif annotation_type.startswith("I-"):
            self.end = end
        elif self.annotation_type:
            result.append(self.yield_annotation(sentence))
            self.annotation_type = None

        return result




with open("ner_dataset.csv.zst", "rb") as f:
    content = loads(f.read()).decode("latin1")

    my_sentence = ""
    document = []
    quote_open = False
    last = ""
    current_annotations = []
    corpus_annotations = {}
    ca = CurrentAnnotation()
    for no, (sentence, word, pos, tag) in enumerate(reader(content.strip().split("\n")[1:])):
        if no > 50000:
            break
        if sentence and my_sentence:
            ca.set_sentence_number(no + 1)
            document = Document(content=my_sentence, key=CORPUS_URL.format(no + 1))
            corpus_annotations[document] = [Annotation(key=a['key'],
                                                       surface_forms=a['surface_form'],
                                                       start_indices=a['start'],
                                                       end_indices=a['end'],
                                                       annotation_type=AnnotationType(a['annotation_type']),
                                                       annotator=ANNOTATOR)
                                            for a in current_annotations if a]
            my_sentence = ""
            quote_open = False
            last = ""
            current_annotations = []

        if my_sentence and (len(word) > 2 or word.isalnum()) and (last != '"' or not quote_open):
            left = " "
        elif word == '"' and not quote_open:
            left = " "
        else:
            left = ""

        if word == '"':
            quote_open = not quote_open

        my_sentence += left
        current_annotations += ca.register_word(len(my_sentence), len(my_sentence) + len(word), tag, my_sentence)
        my_sentence += word
        last = word



r = Run('gmb-gold',
        description='Gronigen Mean Bank Gold Standard Annotations',
        corpus=Corpus('Gronigen Mean Bank', supported_annotation_types=list(ca.supported_annotation_types)),
        document_annotations=corpus_annotations)

n = NifExportFormat()
n.export(r, Path("export.ttl"))


