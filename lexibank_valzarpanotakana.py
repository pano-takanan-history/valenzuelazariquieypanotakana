import attr
import pathlib
from collections import defaultdict
from clldutils.misc import slug
from lingpy import Wordlist
from pylexibank import Dataset as BaseDataset
from pylexibank import progressbar as pb
from pylexibank import Language, Lexeme, Concept
from pylexibank import FormSpec
from pyedictor import fetch

@attr.s
class CustomLanguage(Language):
    NameInSource = attr.ib(default=None)
    SubGroup = attr.ib(default=None)
#    Sources = attr.ib(default=None)


@attr.s
class CustomLexeme(Lexeme):
    Table = attr.ib(default=None)
    Alignment = attr.ib(default=None)
    NumberInSource = attr.ib(default=None)
    #FormFromProto = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "valzarpanotakana"
    language_class = CustomLanguage
    lexeme_class = CustomLexeme
    form_spec = FormSpec(
        separators="~;,/-",
        missing_data=["--", "- -", "-", "-- **", "--.", "- --"],
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
        first_form_only=False
        )

    def cmd_download(self, _):
        print("updating...")
        with open(self.raw_dir.joinpath("data.tsv"), "w", encoding="utf-8") as f:
            f.write(
                fetch(
                    "valzarpanotakana",
                    columns=[
                        "ALIGNMENT",
                        "COGID",
                        "TOKENS",
                        #"SUBGROUP",
                        "CONCEPT",
                        #"PROTO_FORM",
                        #"NUMBERING_IN_SOURCE", #Not quite sure whether we keep this column
                        "DOCULECT",
                        "FORM",
                        "VALUE",
                        "NOTE"
                    ],
                    base_url="http://lingulist.de/edev"
                )
            )

    def cmd_makecldf(self, args):
        args.writer.add_sources()
        args.log.info("added sources")

        # add conceptlists
        concepts = {}
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

        # add language
        languages = args.writer.add_languages(lookup_factory="Name")
        args.log.info("added languages")

        data = Wordlist(str(self.raw_dir.joinpath("data.tsv")))
        data.renumber("CONCEPT", "cogid")

        # add data
        for (
            idx,
            alignment,
            cogid,
            tokens,
            #subgroup,
            concept,
            #proto_form,
            #numbering_in_source,
            doculect,
            form,
            value,
            note
        ) in pb(
            data.iter_rows(
                "alignment",
                "cogid",
                "tokens",
                #"subgroup",
                "concept",
                #"proto_form",
                #"numbering_in_source",
                "doculect",
                "form",
                "value",
                "note"
            ),
            desc="cldfify"
        ):
            lexeme = args.writer.add_form_with_segments(
                Language_ID=languages[doculect],
                Parameter_ID=concepts[(concept)],
                Form=form.strip(),
                    Value=value.strip() or form.strip(),
                Segments=tokens,
                #FormFromProto=proto_form,
                Comment=note,
                Cognacy=cogid,
                Alignment=" ".join(alignment),
                #SubGroup=subgroup,
                #NumberingInSource=numbering_in_source,
                Source="Valenzuela2023"
            )

            args.writer.add_cognate(
                lexeme=lexeme,
                Cognateset_ID=cogid,
                Cognate_Detection_Method="expert",
                Source="Valenzuela2023"
                )
