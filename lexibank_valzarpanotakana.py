import attr
import pathlib
from collections import defaultdict
from clldutils.misc import slug
from lingpy import Wordlist
from pylexibank import Dataset as BaseDataset
from pylexibank import progressbar as pb
from pylexibank import Language, Lexeme, Concept
from pylexibank import FormSpec

@attr.s
class CustomLanguage(Language):
    NameInSource = attr.ib(default=None)
#    Sources = attr.ib(default=None)


@attr.s
class CustomConcept(Concept):
    Proto_Concept = attr.ib(default=None)
    Proto_ID = attr.ib(default=None)
    Original_Concept = attr.ib(default=None)


@attr.s
class CustomLexeme(Lexeme):
    UncertainCognacy = attr.ib(default=None)
    ConceptFromProto = attr.ib(default=None)
    EntryFromProto = attr.ib(default=None)
    FormFromProto = attr.ib(default=None)
    ProtoSet = attr.ib(default=None)
    ConceptInSource = attr.ib(default=None)
    MorphologicalInformation = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "girardprototakanan"
    language_class = CustomLanguage
    concept_class = CustomConcept
    lexeme_class = CustomLexeme
    form_spec = FormSpec(
        separators="~;,",
        missing_data=["--", "- -", "-", "-- **", "--.", "- --"],
        replacements=[
            (" ", "_"),
            ("<-zu>", "-zu"),
            ("<tawa-daʔa>", "tawa-daʔa"),
            ("<{s/z}awa-da>", "Sawa-da"),
            ("{u/o}", "U"),
            ("{e/ä}", "E"),
            ("{a/e}", "A"),
            ("{i/e}", "I"),
            ("{e/i}", "I"),
            ("{a/u}", "Ä")
        ],
        first_form_only=True
        )

    def cmd_makecldf(self, args):
        args.writer.add_sources()
        args.log.info("added sources")

        # add conceptlists
        concepts = defaultdict()

        # Proto Concepts: New
        proto_list = self.etc_dir.read_csv(
            "proto_concepts.tsv",
            delimiter="\t",
            dicts=True
            )

        for concept in proto_list:
            idx = slug(concept["PROTO_CONCEPT"])
            args.writer.add_concept(
                ID=idx,
                Name=concept["PROTO_CONCEPT"],
                Original_Concept=concept["GLOSS"],
                Concepticon_ID=concept["CONCEPTICON_ID"],
                Concepticon_Gloss=concept["CONCEPTICON_GLOSS"],
                Proto_ID=concept["PROTO_ID"]
                )

            concepts[concept["PROTO_CONCEPT"]] = idx

        # Other Concepts
        other_concepts = self.etc_dir.read_csv(
            "other_concepts.tsv",
            delimiter="\t",
            dicts=True
            )

        for concept in other_concepts:
            idx = slug(concept["PROTO_CONCEPT"])
            args.writer.add_concept(
                ID=idx,
                Name=concept["PROTO_CONCEPT"],
                Original_Concept=concept["GLOSS"],
                Proto_ID=concept["PROTO_ID"],
                )
            concepts[concept["PROTO_CONCEPT"]] = idx

        args.log.info("added concepts")

        # add language
        languages = args.writer.add_languages(lookup_factory="ID")
        args.log.info("added languages")

        data = Wordlist(str(self.raw_dir.joinpath("data.tsv")))
        data.renumber(
            "PROTO_FORM", "cogid")

        # add data
        for (
            idx,
            proto_set,
            proto_entry,
            proto_form,
            proto_concept,
            doculect,
            concept,
            value,
            morph_infor,
            note,
            uncertainty,
            source,
            cogid
        ) in pb(
            data.iter_rows(
                "proto_set",
                "proto_entry",
                "proto_form",
                "proto_concept",
                "doculect",
                "concept",
                "value",
                "morph_infor",
                "note",
                "uncertainty",
                "source",
                "cogid"
            ),
            desc="cldfify"
        ):
            for lexeme in args.writer.add_forms_from_value(
                    Language_ID=languages[doculect],
                    Parameter_ID=concepts[(proto_concept)],
                    Value=value,
                    EntryFromProto=proto_entry,
                    FormFromProto=proto_form,
                    ConceptFromProto=proto_concept,
                    MorphologicalInformation=morph_infor,
                    Comment=note,
                    Source=source,
                    UncertainCognacy=uncertainty,
                    ProtoSet=proto_set,
                    Cognacy=cogid,
                    ConceptInSource=concept,
                    ):

                args.writer.add_cognate(
                        lexeme=lexeme,
                        Cognateset_ID=cogid,
                        Cognate_Detection_Method="expert",
                        Source="Girard1971"
                        )
