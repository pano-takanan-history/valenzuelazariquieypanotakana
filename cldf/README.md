<a name="ds-cldfmetadatajson"> </a>

# Wordlist CLDF dataset derived from Valenzuela, Pilar and Roberto Zariquiey's "Classificação das línguas na Amazônia Ocidental: avanços a favor da hipótese Pano-Takana" from 2023

**CLDF Metadata**: [cldf-metadata.json](./cldf-metadata.json)

property | value
 --- | ---
[dc:bibliographicCitation](http://purl.org/dc/terms/bibliographicCitation) | Valenzuela, Pilar and Zariquiey, Roberto (2023). Classificação das línguas na Amazônia Ocidental: avanços a favor da hipótese Pano-Takana. LIAMES: Línguas Indígenas Americanas, Campinas, SP.
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF Wordlist](http://cldf.clld.org/v1.0/terms.rdf#Wordlist)
[dc:identifier](http://purl.org/dc/terms/identifier) | https://doi.org/10.20396/liames.v23i00.8670150
[dc:license](http://purl.org/dc/terms/license) | https://creativecommons.org/licenses/by/4.0/
[dcat:accessURL](http://www.w3.org/ns/dcat#accessURL) | https://github.com/pano-tacanan-history/valzarpanotakana
[prov:wasDerivedFrom](http://www.w3.org/ns/prov#wasDerivedFrom) | <ol><li><a href="https://github.com/pano-tacanan-history/valzarpanotakana/tree/cff3b94">pano-tacanan-history/valzarpanotakana cff3b94</a></li><li><a href="https://github.com/glottolog/glottolog/tree/v4.7">Glottolog v4.7</a></li><li><a href="https://github.com/MuffinLinwist/concepticon-data/tree/v3.0.0">Concepticon v3.0.0</a></li><li><a href="https://github.com/cldf-clts/clts/tree/8b92f99">CLTS v1.4-161-g8b92f99</a></li></ol>
[prov:wasGeneratedBy](http://www.w3.org/ns/prov#wasGeneratedBy) | <ol><li><strong>lingpy-rcParams</strong>: <a href="./lingpy-rcParams.json">lingpy-rcParams.json</a></li><li><strong>python</strong>: 3.8.8</li><li><strong>python-packages</strong>: <a href="./requirements.txt">requirements.txt</a></li></ol>
[rdf:ID](http://www.w3.org/1999/02/22-rdf-syntax-ns#ID) | valzarpanotakana
[rdf:type](http://www.w3.org/1999/02/22-rdf-syntax-ns#type) | http://www.w3.org/ns/dcat#Distribution


## <a name="table-formscsv"></a>Table [forms.csv](./forms.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF FormTable](http://cldf.clld.org/v1.0/terms.rdf#FormTable)
[dc:extent](http://purl.org/dc/terms/extent) | 812


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Local_ID](http://purl.org/dc/terms/identifier) | `string` | 
[Language_ID](http://cldf.clld.org/v1.0/terms.rdf#languageReference) | `string` | References [languages.csv::ID](#table-languagescsv)
[Parameter_ID](http://cldf.clld.org/v1.0/terms.rdf#parameterReference) | `string` | References [parameters.csv::ID](#table-parameterscsv)
[Value](http://cldf.clld.org/v1.0/terms.rdf#value) | `string` | 
[Form](http://cldf.clld.org/v1.0/terms.rdf#form) | `string` | 
[Segments](http://cldf.clld.org/v1.0/terms.rdf#segments) | list of `string` (separated by ` `) | 
[Comment](http://cldf.clld.org/v1.0/terms.rdf#comment) | `string` | 
[Source](http://cldf.clld.org/v1.0/terms.rdf#source) | list of `string` (separated by `;`) | 
`Cognacy` | `string` | 
`Loan` | `boolean` | 
`Graphemes` | `string` | 
`Profile` | `string` | 
`Table` | `string` | 
`NumberInSource` | `string` | 
`FormFromProto` | `string` | 

## <a name="table-languagescsv"></a>Table [languages.csv](./languages.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF LanguageTable](http://cldf.clld.org/v1.0/terms.rdf#LanguageTable)
[dc:extent](http://purl.org/dc/terms/extent) | 32


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Glottocode](http://cldf.clld.org/v1.0/terms.rdf#glottocode) | `string` | 
`Glottolog_Name` | `string` | 
[ISO639P3code](http://cldf.clld.org/v1.0/terms.rdf#iso639P3code) | `string` | 
[Macroarea](http://cldf.clld.org/v1.0/terms.rdf#macroarea) | `string` | 
[Latitude](http://cldf.clld.org/v1.0/terms.rdf#latitude) | `decimal` | 
[Longitude](http://cldf.clld.org/v1.0/terms.rdf#longitude) | `decimal` | 
`Family` | `string` | 
`NameInSource` | `string` | 

## <a name="table-parameterscsv"></a>Table [parameters.csv](./parameters.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF ParameterTable](http://cldf.clld.org/v1.0/terms.rdf#ParameterTable)
[dc:extent](http://purl.org/dc/terms/extent) | 49


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Name](http://cldf.clld.org/v1.0/terms.rdf#name) | `string` | 
[Concepticon_ID](http://cldf.clld.org/v1.0/terms.rdf#concepticonReference) | `string` | 
`Concepticon_Gloss` | `string` | 

## <a name="table-cognatescsv"></a>Table [cognates.csv](./cognates.csv)

property | value
 --- | ---
[dc:conformsTo](http://purl.org/dc/terms/conformsTo) | [CLDF CognateTable](http://cldf.clld.org/v1.0/terms.rdf#CognateTable)
[dc:extent](http://purl.org/dc/terms/extent) | 812


### Columns

Name/Property | Datatype | Description
 --- | --- | --- 
[ID](http://cldf.clld.org/v1.0/terms.rdf#id) | `string` | Primary key
[Form_ID](http://cldf.clld.org/v1.0/terms.rdf#formReference) | `string` | References [forms.csv::ID](#table-formscsv)
[Form](http://linguistics-ontology.org/gold/2010/FormUnit) | `string` | 
[Cognateset_ID](http://cldf.clld.org/v1.0/terms.rdf#cognatesetReference) | `string` | 
`Doubt` | `boolean` | 
`Cognate_Detection_Method` | `string` | 
[Source](http://cldf.clld.org/v1.0/terms.rdf#source) | list of `string` (separated by `;`) | 
[Alignment](http://cldf.clld.org/v1.0/terms.rdf#alignment) | list of `string` (separated by ` `) | 
`Alignment_Method` | `string` | 
`Alignment_Source` | `string` | 

