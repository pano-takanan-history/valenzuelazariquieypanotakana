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
class CustomLexeme(Lexeme):
    Table = attr.ib(default=None)
    NumberInSource = attr.ib(default=None)
    FormFromProto = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "valzarpanotakana"
    language_class = CustomLanguage
    lexeme_class = CustomLexeme
    form_spec = FormSpec(
        separators="~;,",
        missing_data=["--", "- -", "-", "-- **", "--.", "- --"],
        replacements=[
            (" ", "_"),
            ("[i]", "i"),
        ],
        first_form_only=True
        )

    def cmd_makecldf(self, args):
        args.writer.add_sources()
        args.log.info("added sources")

        # add conceptlists
        concepts = {}
        for concept in self.concepts:
            idx = concept["NUMBER"]+"_"+slug(concept["GLOSS"])
            concepts[concept["GLOSS"]] = idx
            args.writer.add_concept(
                ID=idx,
                Name=concept["GLOSS"],
                Concepticon_ID=concept["CONCEPTICON_ID"],
                Concepticon_Gloss=concept["CONCEPTICON_GLOSS"]
            )


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
            number,
            number_in_source,
            concept,
            proto_form,
            doculect,
            value,
            note,
            cogid
        ) in pb(
            data.iter_rows(
                "number",
                "number_in_source",
                "concept",
                "proto_form",
                "doculect",
                "value",
                "note",
                "cogid"
            ),
            desc="cldfify"
        ):
            for lexeme in args.writer.add_forms_from_value(
                    Language_ID=languages[doculect],
                    Parameter_ID=concepts[(concept)],
                    Value=value,
                    FormFromProto=proto_form,
                    Comment=note,
                    Cognacy=cogid,
                    ):

                args.writer.add_cognate(
                        lexeme=lexeme,
                        Cognateset_ID=cogid,
                        Cognate_Detection_Method="expert",
                        Source="Valenzuela2023"
                        )
