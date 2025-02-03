from pathlib import Path

import pytest

from koheesio.spark import DataFrame, SparkSession  # type: ignore
from koheesio.spark.transformations.http_request import HttpRequestTransformation  # type: ignore

from koheesio.spark.transformations.http_request import url_parts


@pytest.fixture
def input_df(spark: SparkSession) -> DataFrame:
    """A simple DataFrame containing two URLs."""
    return spark.createDataFrame(
        [
            (101, "http://www.textfiles.com/100/adventur.txt"),
            (102, "http://www.textfiles.com/100/arttext.fun"),
        ],
        ["key", "url"],
    )


@pytest.fixture
def download_path(tmp_path: Path) -> Path:
    _path = tmp_path / "downloads"
    _path.mkdir(exist_ok=True)
    return _path


class TestHttpRequestTransformation:
    """
    Input DataFrame:

    | key | url                                        |
    |-----|--------------------------------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  |
    | 102 |	http://www.textfiles.com/100/arttext.fun   |

    Output DataFrame:

    | key | url                                        | downloaded_file_path  |
    |-----|--------------------------------------------|-----------------------|
    | 101 |	http://www.textfiles.com/100/adventur.txt  | downloads/adventur.txt|
    | 102 |	http://www.textfiles.com/100/arttext.fun   | downloads/arttext.fun |

    """

    test_data_url_regex = [
        # url, expected_protocol, expected_base_url, expected_port, expected_rest
        ("https://example.com:443/some/random/url", "https", "example.com", "443", "/some/random/url"),
        ("http://something.a.bit.more.complicated:9999/foo/bar", "http", "something.a.bit.more.complicated", "9999", "/foo/bar"),
        ("https://no-port-example.ext/42/index.jsp?foo=bar&baz=bla", "https", "no-port-example.ext", None, "/42/index.jsp?foo=bar&baz=bla"),
        ("ftp://ftp.example.com/resource.txt", "ftp", "ftp.example.com", None, "/resource.txt"),
        ("http://localhost:8080/test", "http", "localhost", "8080", "/test"),
        ("https://sub.domain.example.com/path/to/resource?query=string&another=value", "https", "sub.domain.example.com", None, "/path/to/resource?query=string&another=value"),
        ("http://192.168.1.1:8080/admin", "http", "192.168.1.1", "8080", "/admin"),
        ("https://user:password@example.com:8443/path?query=param#fragment", "https", "user:password@example.com", "8443", "/path?query=param#fragment"),
        ("http://example.org", "http", "example.org", None, None),
        ("https://example.net/path/to/resource.html", "https", "example.net", None, "/path/to/resource.html"),
        ("http://example.com:80/path/to/resource?query=param", "http", "example.com", "80", "/path/to/resource?query=param"),
        ("custom_protocol://base_url:foo/rest", "custom_protocol", "base_url:foo", None, "/rest"),
        ("WEBDAV://example.com:8080/path/to/resource", "WEBDAV", "example.com", "8080", "/path/to/resource"),
    ]

    @pytest.mark.parametrize("url, expected_protocol, expected_base_url, expected_port, expected_path", test_data_url_regex)
    def test_url_parts(self, url, expected_protocol, expected_base_url, expected_port, expected_path):
        match = url_parts.match(url)

        assert match is not None, f"Failed to match {url}"
        assert match.group("protocol") == expected_protocol, f"Failed to match protocol: {match.groupdict()}"
        assert match.group("port") == expected_port, f"Failed to match port: {match.groupdict()}"
        assert match.group("base_url") == expected_base_url, f"Failed to match base_url: {match.groupdict()}"
        assert match.group("path") == expected_path, f"Failed to match rest: : {match.groupdict()}"

    def test_downloading_files(self, input_df: DataFrame, download_path: Path) -> None:
        """Test that the files are downloaded and the DataFrame is transformed correctly."""
        # Arrange
        expected_data = [
            '\r\n\t       ***************************************\r\n\t       *\t\t\t\t     *\r\n\t       *\t      ADVENTURE \t     *\r\n\t       *\t\t\t\t     *\r\n\t       *       Solving it in easy steps      *\r\n\t       *\t\t\t\t     *\r\n\t       *\t\t\t\t     *\r\n\t       ***************************************\r\n\t\t      FROM: THE ROM RAIDER\r\n\t\t\t    DR. DIGITAL\r\n\t\t  CALL HER MAJESTY\'S SECRET SERVICE\r\n\t\t      3 0 3 - 7 5 1 - 2 0 6 3\r\n\r\nWARNING:  THIS WALK-THRU SHOWS HOW TO SOLVE THIS ADVENTURE STEP BY STEP, THESE\r\nARE NOT HINTS!\r\nSPECIFIC INSTRUCTIONS ARE ENCLOSED IN QUOTES, SO TYPE WHAT YOU SEE.\r\nSTART OFF BY GOING "N".  YOU ARE NOW STANDING AT THE END OF A ROAD BEFORE A\r\nSMALL BRICK BUILDING.  THIS BUILDING IS THE WELLHOUSE, AND IS AN IMPORTANT PART\r\nOF THE GAME.  THE OBJECT OF THIS GAME IS TO GET THE 15 TREASURES BACK TO THE\r\nWELLHOUSE.\r\nCONTINUE WITH "IN", AND YOU ARE INSIDE THE WELL HOUSE, SO "GET FOOD", "GET\r\nLAMP", "GET KEYS", "GET BOTTLE".  NOW, THERE ARE THREE MAGIC WORDS YOU NEED TO\r\nUSE.  THEY ARE PLUGH, XYZZY AND PLOVER.  TWO OF THESE WORDS ARE USED TO GET TO\r\nAND FROM THE WELLHOUSE AND PARTS OF THE CAVE.  THE THIRD, PLOVER IS USED TO GET\r\nBETWEEN TWO PARTS OF THE CAVE.\tANOTHER THING TO REMEMBER IS THAT YOUR LAMP\r\nLIGHT IS LIMITED, SO TURN OFF YOUR LAMP WHEN YOU DO NOT NEED IT!\r\nNOW ON TO THE CAVE.  SAY "PLUGH", THEN "LAMP ON", "S", "GET SILVER", "DROP\r\nKEYS", "DROP FOOD", "DROP BOTTLE", "N".  YOU ARE NOW BACK AT Y2.  HERE IS WHERE\r\nYOU USE PLOVER.  SO SAY "PLOVER", THEN "NE", "GET PYRAMID", "S", "LAMP OFF",\r\n"DROP LAMP", "DROP SILVER", "DROP PYRAMID", "GET EMERALD", "W", "DROP EMERALD",\r\n"BACK".  THE REASON YOU DID ALL OF THAT WAS THAT THE ONLY WAY TO GET THE\r\nEMERALD OUT OF THE PLOVER ROOM IS OUT THROUGH THE CRACK, BUT YOU CAN NOT CARRY\r\nANYTHING ELSE WITH YOU.\r\nNOW "GET LAMP", "GET SILVER", "GET PYRAMID", THEN "PLOVER" AND "PLUGH".  YOU\r\nARE NOW BACK IN THE WELLHOUSE, SO DROP OFF YOUR TREASURES SO FAR.  TYPE "DROP\r\nSILVER", "DROP PYRAMID".  BACK TO THE CAVE WITH "XYZZY", AND "LAMP ON", "E",\r\n"GET CAGE", "W", "GET ROD", "W", "W","DROP ROD", "GET BIRD", "GET ROD", "W",\r\n"D","W", "WAVE ROD".  THERE IS NOW A CRYSTAL BRIDGE ACROSS THE CHASM.  CONTINUE\r\nWITH "DROP ROD", THEN GO "W", "GET DIAMONDS", "E", "E", "S", "GET GOLD", "N",\r\n"D".  AT THIS POINT YOU WILL SEE A HUGE GREEN SNAKE BARING YOUR WAY.  TO BE RID\r\nOF THE SNAKE, "DROP BIRD", AND THE SNAKE IS DRIVEN AWAY.\r\nAT THIS POINT I NEED TO EXPLAIN SOMETHING ELSE.  DURING THE COURSE OF THE\r\nGAME, A DWARF WILL SEEK YOU OUT AND THROW AN AXE AT YOU.  GET THE AXE.\tTHE\r\nNEXT TIME YOU SEE THE DWARF, THROW THE AXE AT HIM TO KILL HIM.\tTHIS MAY HAPPEN\r\nSEVERAL TIMES, SO JUST KEEP KILLING THE DWARVES.  ANOTHER PERSON TO WATCH OUT\r\nFOR IS THE PIRATE.  HE WILL SHOW UP AT SOME POINT IN THE GAME, AND STEAL\r\nWHATEVER TREASURES YOU ARE CARRYING.  IF THIS HAPPENS, YOU CAN GET THE\r\nTREASURES BACK WHEN YOU FIND HIS TREASURE CHEST.\r\nCONTINUE WITH "DROP CAGE", "SW", "W".  HERE IS A DRAGON SITTING ON A PERSIAN\r\nRUG.  YOU NEED TO KILL THE DRAGON TO GET THE RUG SO TYPE "KILL DRAGON".  THE\r\nGAME WILL RESPOND WITH "WITH WHAT?  YOUR BARE HANDS?".  ANSWER "YES".\r\nCONGRATULATIONS, YOU HAVE NOW KILLED THE DRAGON!.  SO, "GET RUG", THEN "E",\r\n"E", "S", "GET JEWELRY", "N", "W", "GET COINS", "E", "N", "N", "LAMP OFF", AND\r\n"PLUGH".  YOU ARE AGAIN BACK IN THE WELLHOUSE, SO DROP YOUR TREASURES WITH\r\n"DROP DIAMONDS", "DROP GOLD", "DROP RUG", "DROP JEWELRY", "DROP COINS".  BACK\r\nTO THE CAVE WITH "PLUGH", THEN "LAMP ON", "S", "GET FOOD", "GET KEYS", "GET\r\nBOTTLE", "D", "W", "D", "W", "W", THEN SAY "ORIENTAL" TO GET TO THE ORIENTAL\r\nROOM.  THEN "GET VASE", "W", "DROP KEYS", "DROP FOOD", "BACK", "N", "W", AND\r\nYOU HAVE NOW FOUND THE EMERALD YOU PREVIOUSLY PLACED THERE.  SO "GET EMERALD",\r\n"NW", "S", "SE", "E", "GET PILLOW".  THE PILLOW IS USED TO PLACE THE VASE ON,\r\nSO YOU DON\'T BREAK IT.\r\nNOW GO "BACK", "W", "W", "D", "WATER PLANT", "U".  THE PLANT IS CRYING FOR MORE\r\nWATER, SO NOW YOU NEED TO GO GET IT SOME WATER.  BUT BEFORE YOU DO THAT DROP\r\nOFF YOUR TREASURES AT THE WELL- HOUSE BY GOING "E", "E", "NE", "E", "U", "E",\r\n"U", "N", "LAMP OFF", "PLUGH" AND YOU ARE BACK IN THE WELLHOUSE.  SO "DROP\r\nPILLOW", "DROP VASE", "DROP EMERALD".\r\nBACK TO THE CAVE WITH "PLUGH", "LAMP ON", "S", "D", "E", "D", "FILL BOTTLE".\r\nYOUR BOTTLE IS NOW FILLED WITH WATER, SO GO "U", "W", "W", "D", "W", "W", "W",\r\n"W", "D", "WATER PLANT", "U", "E", "D", "GET OIL", "U", "W", "D".  NOW YOU CAN\r\nCLIMB THE BEANSTALK SO, "CLIMB", "W", "GET EGGS", "N", "OIL DOOR", "DROP\r\nBOTTLE", "N", "GET TRIDENT", "W", "D", "DROP TRIDENT", "GET KEYS", "GET FOOD",\r\n"SW", "U".  YOU ARE NOW IN FRONT OF A BRIDGE WITH A TROLL GUARDING IT.  THE\r\nONLY THING THAT WILL CAUSE THE TROLL TO LEAVE IS TO THROW HIM A TREASURE.\r\nTHERE IS ONLY ONE TREASURE YOU CAN THROW HIM AND THAT IS THE EGGS.  SO "THROW\r\nEGGS", THEN "CROSS".\r\nYOU ARE NOW ACROSS THE CHASM.  CONTINUE WITH "NE", "E", "NE", "E", "GET\r\nSPICES", "W", "N", (HERE IS THE VOLCANO), "S", "S", "SE", "S", "IN".  YOU ARE\r\nNOW IN A ROOM WITH A FEROCIOUS BEAR, WHO IS LOCKED UP WITH A GOLD CHAIN.  YOU\r\nNEED TO GET THE CHAIN, SO "FEED BEAR", THEN "UNLOCK" THE CHAIN, "GET BEAR",\r\n"GET CHAIN", "DROP KEYS", AND "OUT".  THE BEAR WILL NOW FOLLOW YOU WHEREVER YOU\r\nGO.  SO GO "U", "U", "W", "W", AND "CROSS".  FROM NOWHERE, THE TROLL APPEARS\r\nAGAIN.\tJUST "DROP BEAR", AND THE TROLL WILL DISAPPEAR.  NOW "CROSS" THE\r\nBRIDGE, THEN GO "SW", "D", "GET TRIDENT", "SE", "SE", "NE", "E", "N".  YOU ARE\r\nIN THE CLAM ROOM, SO NOW "OPEN CLAM".  (IT IS NECESSARY TO HAVE THE TRIDENT\r\nBEFORE YOU CAN OPEN THE CLAM).\tNOW YOU NEED TO GO AFTER THE PEARL SO GO "D",\r\n"D", "GET PEARL".  THEN "U", "U", "S", "U", "E", "U", "N", "LAMP OFF", AND SAY\r\n"PLUGH".  THEN "DROP CHAIN", "DROP PEARL", "DROP TRIDENT", "DROP SPICES".\r\nBACK TO THE CAVE WITH "PLUGH", THEN "LAMP ON", "S", "D", "W", "D", "W", "W",\r\n"W", "W", "D", "CLIMB", "W".  YOU ARE NOW BACK AT THE SPOT WHERE YOU ORIGINALLY\r\nFOUND THE EGGS WHICH YOU THREW TO THE TROLL.  THE REASON YOU THREW THE EGGS WAS\r\nBECAUSE YOU CAN GET THEM BACK AGAIN!  JUST SAY "FEE", "FIE", "FOE", "FOO", AND\r\nTHEY MAGICALLY APPEAR!\tSO "GET EGGS", THEN "N", "N", "W", "D", "SE", "SE",\r\n"NE", "E", "U", "E", "U", "N", "LAMP OFF", AND "PLUGH".  THEN "DROP EGGS".\r\nNOW ON TO FIND THE LAST TREASURE, THE PIRATE\'S CHEST!  GO THERE WITH "PLUGH",\r\n"LAMP ON", "E", "U", "W", "W", "W", "S", "E", "S", "S", "S", "N", "E", "E",\r\n"NW", "GET CHEST", "BACK", "N", "D", "E", "E", "XYZZY".  YOU ARE BACK AT THE\r\nBUILDING, SO "DROP CHEST".  CONGRATULATIONS, YOU NOW HAVE ALL 15 TREASURES!\r\nAT THIS POINT YOU HAVE ONE MORE THING TO ACCOMPLISH BEFORE YOU GO THE THE GRAND\r\nFINALE.  SO SAY "XYZZY", THEN GO "W", "W", "W", "D", "D", "N", "D", "W", "D",\r\n"E", "GET MAGAZINES".  YOUR QUEST IS TO TAKE THE MAGAZINES, AND PLACE THEM JUST\r\nINSIDE WITT\'S END.  THIS ACTION IS OFTEN MISSED BY PEOPLE PLAYING THIS GAME,\r\nAND AS A RESULT, THEY DO NOT FINISH THE GAME WITH ALL 350 POINTS AND THE GRAND\r\nMASTER STATUS.\tNOW GO "E", THEN "DROP MAGAZINES", AND "BACK".\r\nNOW COMES THE EASY PART, JUST WANDER AROUND THESE PASSAGES, AS YOU NOW HAVE\r\nSOME TIME TO KILL.  THE GAME GIVES YOU A SET NUMBER A MOVES AFTER ALL THE\r\nTREASURES ARE IN THE WELLHOUSE, AND THEN YOU ARE TRANSPORTED TO THE GRAND\r\nFINALE.  THE MESSAGE YOU WILL GET WILL START BY SAYING "THE CAVE IS NOW\r\nCLOSED", ECT.  AT THIS POINT YOU ARE TRANSPORTED TO THE NORTHEAST END OF AN\r\nIMMENSE ROOM.  DO THE FOLLOWING:  "SW", "GET ROD", "NE", "DROP ROD", "SW".\r\nWHAT YOU HAVE DONE IS PLACED A STICK OF DYNAMITE AT THE OPPOSITE END OF THE\r\nROOM.  NOW YOU SAY THE MAGIC WORD - "BLAST".  YOU HAVE NOW EXPLODED A HOLE IN\r\nTHE FAR WALL, AND YOU MARCH THROUGH IT INTO THE MAIN OFFICE.  YOU HAVE NOW\r\nFINISHED THE GAME OF ADVENTURE!\r\n\x1a',
            "+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+\r\n^+\t\t\t\t\t\t\t\t\t   +^\r\n+^\t\t\tTHE ART OF WRITING TEXTFILES\t\t\t   ^+\r\n^+\t\t\t\t\t\t\t\t\t   +^\r\n+^\t\t\t\tWRITTEN BY:\t\t\t\t   ^+\r\n^+\t\t\t     THE BRONZE RIDER\t\t\t\t   +^\r\n+^\t\t      HOME BASE:  THE PRISM BBS/AE/CF\t\t\t   ^+\r\n^+\t\t\t\t\t\t\t\t\t   +^\r\n+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+^+\r\nTHIS FILE IS OBVIOUSLY ABOUT WRITING T-FILES.  TO WRITE A T-FILE, YOU NEED\r\nTHESE THINGS:\r\n1.  YOU NEED AN APPLE //E OR //C WITH UPPER & LOWER CASE...\r\n    (USING A II+ IS REALLY A PAIN.)  AN APPLE ISN'T NECESSARY, BUT REAL\r\n    TEXTFILE WRITERS DONT USE ANYTHING BUT THE BEST...\r\n2.  YOU NEED AN 80 COLUMN CARD.\r\n3.  YOU NEED THE ABILITY TO STRETCH OUT WHAT YOU ARE TRYING TO SAY SO THAT\r\n    THE FILE TAKES LONGER TO V)IEW AND TAKES UP MORE DISK SPACE.  A TEXTFILE\r\n    SHOULD BE AT LEAST 15 SECTORS, BUT ITS BEST IF ITS OVER 20.\r\n    EXAMPLE:\r\n      YOU WANT TO SAY:\r\n\t  WRITING T-FILES IS A PAIN...\r\n      IN A T-FILE YOU WOULD SAY IT LIKE THIS:\r\n\t  WRITING A TEXTFILE IS A VERY TIME-CONSUMING AND ARDUOUS TASK.\r\n\t  IT TAKES A LOT OF THOUGHT AND IS VERY WEARING ON A PERSON.  IT\r\n\t  IS NOT AT ALL EASY.\r\n      AND THEN YOU ELABORATE ON WHAT YOU JUST SAID.\r\n4.  YOU NEED THE ABILITY TO LOOK AT THE SCREEN AND KNOW EXACTLY WHERE THE\r\n    CENTER IS SO THAT YOU CAN CENTER GARBAGE.\r\n    EX.\r\n\t\t\t       ->VVVVVVVVV<-\r\n\t\t\t       -> GARBAGE <-\r\n\t\t\t       ->^^^^^^^^^<-\r\n    I DON'T HAVE THAT ABILITY, BUT SOMETIMES I GET LUCKY.\r\n5.  YOU NEED TO THE ABILITY TO MIX DIFFERENT CHARACTERS IN AN APPEALLING\r\n    WAY TO MAKE A BORDER FOR THE TITLE.\r\n    EX.\r\n\t+^+^+^+^+^+^+\t    ><><><><><><>\r\n\t^+  HELLO  +^\t    <>\tHELLO  <>\r\n\t+^+^+^+^+^+^+\t    ><><><><><><>\r\n\t\t  (^^^^^^^^^^^)\r\n\t\t   )  HELLO  (\r\n\t\t  (^^^^^^^^^^^)\r\n\t    ><L>  THE LOCKSMITH  <S><\r\nV^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V^V\r\n    AND STUFF LIKE THAT.  THERE ARE A LOT MORE, BUT THEY'RE HARD TO MAKE\r\n    ON AN APPLE II+ SINCE A II+ DOESN'T HAVE ALL THE CHARATERS ON THE\r\n    KEYBOARD.\r\n6.  YOU HAVE TO HAVE A LOT OF PATIEN\x00CE.  YOU HAVE TO BE ABLE TO TYPE FOR\r\n    HOURS AT A TIME.  IF YOU TYPE FAST ENOUGH, YOU DON'T HAVE TO WORRY\r\n    ABOUT THIS STEP.\r\n7.  IT HELPS IF YOU ARE ONE OF THE NEON KNIGHTS\r\n    OF METALAND.\r\n    ><> THOSE GUYS WRITE THE BEST T-FILES <><\r\n8.  ITS HELPFUL TO HAVE A GOOD WORD PROCESSOR, BUT AE WILL DO THE JOB GOOD\r\n    ENOUGH.  AE HAS ONE OF THE BEST TEXT EDITORS.\r\n9.  IT HELPS A LOT IF YOU ARE DRUNK AND TO HAVE MUSIC BLASTING IN YOUR EARS\r\n    IN STEREO.\tI DON'T KNOW WHY, BUT THATS JUST THE WAY IT WORKS.\r\n\r\n    WRITE A TEXT FILE AND GIVE IT TO SOMEONE ELSE WITH A MODEM, BUT THATS\r\n    KIND OF A PAIN.\r\n\r\n\r\nNOW HERE IS A LIST OF NON-NECESSITIES:\r\n\r\n1.  CONTRARY TO POPULAR BELIEF, YOU DON'T HAVE TO SPEEL STUPH RITE.\r\n    SPEELINK ERRURS AR VARY COMON.\r\n2.  YOU DON'T NEED A 1200 BAUD APPLE-CAT UNLESS YOU REALLY KNOW HOW  TO\r\n    STRETCH OUT WHAT YOU ARE TRYING TO SAY.  USUALLY ANY 300 BAUD MODEM\r\n    WILL WORK FINE.\r\n3.  YOU DON'T NEED TO KNOW ANYTHING ABOUT WHAT YOU ARE WRITING YOUR TEXTFILE\r\n    ABOUT IF YOU CAN MAKE IT UP AS YOU GO ALONG.\r\n4.  YOU DON'T NEED TO BE A -REAL PIRATE-, BUT IT HELPS.  YOU ALSO DON'T -HAVE-\r\n    TO BE STONED, BUT IT HELPS.\r\n5.  AND YOU DEFINATELY DON'T NEED A HIGH I.Q., BUT I HEAR THAT IT HELPS.\r\n(^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^)\r\n )!\t\t\t\t\t\t\t\t\t   !(\r\n(!\t\t   CALL THESE AWESOME BBS'S, AE'S, AND CF'S...              !)\r\n )!\t\t\t\t\t\t\t\t\t   !(\r\n(!\t THE PRISM BBS/AE/CF 300/1200 BAUD  10 MEG ....... (201)-637-6715   !)\r\n )!\t THE METAL AE - PASSWORD:KILL .................... (201)-879-6668  !(\r\n(!\t THE CORELINE BBS ................................ (201)-239-7737   !)\r\n )!\t THE DRAGON'S WEYR BBS  10 MEG ................... (201)-992-0834  !(\r\n(!\t\t\tLATER, THE BRONZE RIDER...\t\t\t    !)\r\n )!\t\t\t\t       \x00\t\t\t\t    !(\r\n(^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^)\r\n\x1a",
        ]

        # Act
        transformed_df = HttpRequestTransformation(
            column="url",
            target_column="content",
        ).transform(input_df)
        actual_data = sorted(
            [row.asDict()["content"] for row in transformed_df.select("content").collect()]
        )

        # Assert

        assert transformed_df.count() == 2
        assert transformed_df.columns == ["key", "url", "content"]

        # check that the rows of the output DataFrame are as expected
        assert actual_data == expected_data
