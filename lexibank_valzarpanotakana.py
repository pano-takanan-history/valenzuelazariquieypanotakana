import attr
import pathlib
from collections import defaultdict
from clldutils.misc import slug
from lingpy import Wordlist
from pylexibank import Dataset as BaseDataset
from pylexibank import progressbar as pb
from pylexibank import Language, Lexeme
from pylexibank import FormSpec


@attr.s
class CustomLanguage(Language):
    SubGroup = attr.ib(default=None)


@attr.s
class CustomLexeme(Lexeme):
    Table = attr.ib(default=None)
    NumberInSource = attr.ib(default=None)
    FormFromProto = attr.ib(default=None)
    ConceptInSource = attr.ib(default=None)
    Shell = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "valzarpanotakana"
    language_class = CustomLanguage
    lexeme_class = CustomLexeme
    form_spec = FormSpec(
        separators="~,/",
        missing_data=["--", "- -", "-- **", "--.", "- --"],
        replacements=[
            (" ", "_"),
            ("[i]", "i"),
            ("[", ""),
            ("]", ""),
            ("[...]", ""),
            ("...", ""),
            ("CV", ""),
            ("V", "")
        ],
        first_form_only=True
        )

    def cmd_makecldf(self, args):
        args.writer.add_sources()
        args.log.info("added sources")

        # add conceptlists
        concepts = defaultdict()
        for concept in self.concepts:
            idx = concept["NUMBER"]+"_"+slug(concept["ENGLISH"])
            concepts[concept["ENGLISH"]] = idx
            args.writer.add_concept(
                ID=idx,
                Name=concept["ENGLISH"],
                Concepticon_ID=concept["CONCEPTICON_ID"],
                Concepticon_Gloss=concept["CONCEPTICON_GLOSS"]
            )

        args.log.info("added concepts")

        languages = defaultdict()
        for language in self.languages:
            args.writer.add_language(
                    ID=language["Name"],
                    Name=language["NameInSource"],
                    Glottocode=language["Glottocode"],
                    SubGroup=language["SubGroup"]
                    )
            languages[language["ID"]] = language["Name"]
        args.log.info("added languages")

        data = Wordlist(str(self.raw_dir.joinpath("data.tsv")))
        data.renumber("PROTO_CONCEPT", "cogid")

        # add data
        for (
            idx,
            table,
            id,
            row_in_source,
            proto_concept,
            concept,
            number_shell,
            proto_form,
            doculect,
            value,
            cogid,
            note
        ) in pb(
            data.iter_rows(
                "table",
                "id",
                "row_in_source",
                "proto_concept",
                "concept",
                "number_shell",
                "proto_form",
                "doculect",
                "value",
                "cogid",
                "note"
            ),
            desc="cldfify"
        ):
            for lexeme in args.writer.add_forms_from_value(
                    Language_ID=languages[doculect],
                    Parameter_ID=concepts[(proto_concept)],
                    ConceptInSource=concept,
                    Shell=number_shell,
                    Value=value,
                    FormFromProto=proto_form,
                    Comment=note,
                    Cognacy=cogid,
                    Source="Valenzuela2023"
                    ):
                args.writer.add_cognate(
                        lexeme=lexeme,
                        Cognateset_ID=cogid,
                        Cognate_Detection_Method="expert",
                        Source="Valenzuela2023"
                        )
