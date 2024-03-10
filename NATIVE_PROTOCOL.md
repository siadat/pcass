Running the cassandra 5.0-beta1, the native_protocol_version is 5.

    ~/src/pcass $ docker exec -it cass5_zig cqlsh
    WARNING: cqlsh was built against 5.0-beta1, but this server is 5.0.  All features may not work!
    Connected to Test Cluster at 127.0.0.1:9042
    [cqlsh 6.2.0 | Cassandra 5.0-beta1 | CQL spec 3.4.7 | Native protocol v5]
    Use HELP for help.
    cqlsh> SELECT * FROM system.local ;

     key   | bootstrapped | broadcast_address | broadcast_port | cluster_name | cql_version | data_center | gossip_generation | host_id                              | listen_address | listen_port | native_protocol_version | partitioner                                 | rack  | release_version | rpc_address | rpc_port | schema_version                       | tokens                                                                                                                                                                                                                                                                                                                                                                                  | truncated_at
    -------+--------------+-------------------+----------------+--------------+-------------+-------------+-------------------+--------------------------------------+----------------+-------------+-------------------------+---------------------------------------------+-------+-----------------+-------------+----------+--------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     local |    COMPLETED |        172.17.0.2 |           7000 | Test Cluster |       3.4.7 | datacenter1 |        1709162606 | ed6748fd-8e44-4f4a-b976-ac70408c81df |     172.17.0.2 |        7000 |                       5 | org.apache.cassandra.dht.Murmur3Partitioner | rack1 |       5.0-beta1 |  172.17.0.2 |     9042 | d03783d7-b468-3c1a-82f1-8e30b2edde8b | {'-1368822135264362311', '-2104737827975255951', '-289449327816760654', '-3180493709200835491', '-3998165641813186285', '-5016017046555613301', '-6087517662895646113', '-7527785892798229353', '-8372162540289391111', '1921434920942366817', '3511439473456265455', '5130462769153308752', '6108608221669422828', '7887242522493009956', '8983284422727948245', '954070677863831318'} | {176c39cd-b93d-33a5-a218-8eb06a56f66e: 0x0000018df207a09e000002ab0000018df207ad29, 618f817b-005f-3678-b8a4-53f3930b8e86: 0x0000018df207a09e000002440000018df207ad07, 62efe31f-3be8-310c-8d29-8963439c1288: 0x0000018df207a09e000001d50000018df207ab5e}

    (1 rows)

Running the cassandra 4.1.4, the native_protocol_version is 5.

    ~/src/pcass $ docker exec -it cass4_zig cqlsh
    Connected to Test Cluster at 127.0.0.1:9042
    [cqlsh 6.1.0 | Cassandra 4.1.4 | CQL spec 3.4.6 | Native protocol v5]
    Use HELP for help.
    cqlsh> SELECT * FROM system.local ;

     key   | bootstrapped | broadcast_address | broadcast_port | cluster_name | cql_version | data_center | gossip_generation | host_id                              | listen_address | listen_port | native_protocol_version | partitioner                                 | rack  | release_version | rpc_address | rpc_port | schema_version                       | tokens                                                                                                                                                                                                                                                                                                                                                                                | truncated_at
    -------+--------------+-------------------+----------------+--------------+-------------+-------------+-------------------+--------------------------------------+----------------+-------------+-------------------------+---------------------------------------------+-------+-----------------+-------------+----------+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     local |    COMPLETED |        172.17.0.2 |           7000 | Test Cluster |       3.4.6 | datacenter1 |        1709163500 | d5bd0bb6-5dd6-4923-b4a4-bd91a0b79544 |     172.17.0.2 |        7000 |                       5 | org.apache.cassandra.dht.Murmur3Partitioner | rack1 |           4.1.4 |  172.17.0.2 |     9042 | e09eab28-43fc-3fcc-9f88-9e3791ed9fdd | {'-1638281103533228786', '-2656132508275655802', '-3727633124615688614', '-5167901354518271854', '-6012278002009433611', '-7103575112701645871', '-8199617012936584160', '-820609170920877992', '2070435210463196845', '255146710304701546', '3313955216143788817', '4281319459222324316', '5871324011736222954', '7490347307433266251', '8468492759949380327', '991062403015595187'} | {176c39cd-b93d-33a5-a218-8eb06a56f66e: 0x0000018df215428e000002a70000018df2154f9d, 618f817b-005f-3678-b8a4-53f3930b8e86: 0x0000018df215428e000002400000018df2154f79, 62efe31f-3be8-310c-8d29-8963439c1288: 0x0000018df215428e000001d10000018df2154dd2}

    (1 rows)

Running the cassandra 3.0.29, the native_protocol_version is 4.

    docker exec cass_zig nodetool version

        ReleaseVersion: 3.0.29

    docker exec cass_zig cqlsh -e 'SELECT * FROM system.local'
    
     key   | bootstrapped | broadcast_address | cluster_name | cql_version | data_center | gossip_generation | host_id                              | listen_address | native_protocol_version | partitioner                                 | rack  | release_version | rpc_address | schema_version                       | thrift_version | tokens                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | truncated_at
    -------+--------------+-------------------+--------------+-------------+-------------+-------------------+--------------------------------------+----------------+-------------------------+---------------------------------------------+-------+-----------------+-------------+--------------------------------------+----------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------
     local |    COMPLETED |        172.17.0.2 | Test Cluster |       3.4.0 | datacenter1 |        1709156648 | c8aa015b-7eda-4ca5-8715-c7601b831813 |     172.17.0.2 |                       4 | org.apache.cassandra.dht.Murmur3Partitioner | rack1 |          3.0.29 |     0.0.0.0 | 286d83bc-098a-392f-bccf-243455b0e0fe |         20.1.0 | {'-1063367100709301922', '-1118775069527633787', '-1155412599559404402', '-1175681162142079682', '-1213155159917398347', '-1241429638479943626', '-1324283102669155227', '-1343282151065844231', '-1484853797885419354', '-1512297945600637626', '-1532861937810196011', '-1697537528452451503', '-172567725303215183', '-1752889261198818349', '-1880969958395091332', '-1930599915709577395', '-2027803035756902034', '-2033626970354560157', '-2075871168505000230', '-2200108503071680555', '-2289538904950461307', '-232857017374542523', '-2348204186803979710', '-2531837208720311665', '-2580391564353232345', '-2852733813663777479', '-2918129918485124713', '-3056212264472530076', '-3059086214652215972', '-3149883744829280701', '-3208129470178420591', '-3292623353324828671', '-3293588708510077206', '-3308793685027724013', '-3317042377001216011', '-3355128956150674350', '-3387742755484655717', '-3440511117843300568', '-3486370245858700797', '-3489646105395333273', '-3535751449297317307', '-3555424908377291692', '-3555982606049770132', '-36714052371837603', '-3675730410562797088', '-3689669807750096861', '-3761933816844161152', '-3769237550876483121', '-3806642988795155814', '-3849205944993419905', '-3959864387128879843', '-3981405542832844447', '-4057760581872517070', '-4082572686083835311', '-415726338710818622', '-4181445076311191572', '-4216506501903962672', '-4260417598974935772', '-4307451471144257061', '-4366758925448144529', '-4473915617408476753', '-4477453580106335068', '-4569785136375654434', '-4589843593904193274', '-4606957859905831745', '-5074370843058413579', '-5081556117363219356', '-5128625857679648100', '-5301396202327134210', '-5338343103941691494', '-5468375725542713049', '-5638324337877423063', '-5694547037864308715', '-5726373901397447603', '-5728794821317256303', '-5757439397895581715', '-5871924272141454179', '-5906305554579951002', '-5915721916405082373', '-5980087732259589049', '-5986736090856817050', '-5999749121560817314', '-607387704317605036', '-6083138828596573614', '-6116576934848335870', '-6153425122092696941', '-6211600547169481435', '-6259615231470399857', '-6298195681320140101', '-6387098269322747388', '-644050280905686449', '-6453346558353651209', '-6570942707466871086', '-6656470679546960440', '-6699338315951668277', '-6774230343320630968', '-6818641708432464665', '-6844584831304630300', '-68783700225256843', '-6945512636074540656', '-6989259517850256168', '-7046910507273605856', '-7087300061965827311', '-7129532339037183304', '-7190687417173421094', '-7205672513876851967', '-722615695387630399', '-7297293057643392688', '-7374260801031237865', '-756112074169063093', '-7614535247890452180', '-7664707531062296496', '-776015689472617324', '-7766837189637438264', '-7778271153185828446', '-7878523973596269833', '-7935494770049505971', '-7939569615562992871', '-8019585901857881889', '-8180074078647213579', '-819038353556779836', '-8247497738147119922', '-8285157567731461368', '-8301067587629469841', '-8303925188888881659', '-8314564583097426324', '-8338471585302620071', '-8419642918757475155', '-8511070482400603072', '-8527372298100320782', '-8572560134651308442', '-8581197130900445958', '-8626936537463884875', '-8874992363445574067', '-8918585714381528402', '-8973472346250791436', '-9070678966896871580', '-9101295562154914909', '-9147447414918193332', '-9187776848096416389', '-925503217997877535', '-954163979371801201', '-960316255603252268', '-990392334330456551', '1001651375845643361', '114605081667357869', '1161453960656954749', '1300855332881628293', '1344326154312944474', '148880647574526136', '1544924169554467757', '1577503830079036354', '1584349470041114017', '1603679275853007941', '1660221990543978754', '1661196282310570724', '1711053662629111918', '1949706521029934217', '1973593213289314388', '1995158264235639061', '2050373697161747430', '2053670421434734823', '2134856625983044726', '2149084579523766904', '2210350800596209946', '2330024073947422543', '2390635509973245712', '2522529123859125167', '255277894998682797', '2568834288981746841', '2591049872886928870', '2607716133588209765', '2614571203306261880', '2706173034041914706', '2867921349992938085', '2881171621983041828', '2965742920098393705', '3092763621314116359', '3131085271009782116', '3274809591247700333', '3286098484430169011', '3296800834756105211', '3371495798249307027', '3434862349779722475', '3551142091985899225', '3565644839289141610', '3651605533138604903', '3654213161583685018', '3715528797985427570', '3743738441576102666', '3828747773863528023', '3875200173752512629', '391303167340069392', '3948124570084356392', '4002789142820585941', '4082507709468182646', '4105964098333293610', '4301615647587520899', '465814480839829710', '469517242067424038', '4731722671448877148', '4840699501142555025', '4885480730224933224', '4982665558169482969', '5227311834347039441', '5259691169258978790', '5320421887725278048', '5412333904812136867', '5561535484568612528', '5588090092621517904', '5997705704623775090', '6011593327570107579', '6198636593853615239', '6220547564888621865', '6257646895254464182', '62604930328215709', '6294557144413791601', '6417808048811324808', '6425279020144769421', '6437589096597815049', '6442098642142282546', '6513332665154127862', '6535707558521082630', '6620469812828100805', '6623246529460688703', '6741999325144540105', '6791275984263985683', '6831698689201068634', '6905492883825414785', '6923606674551230113', '7071077557511956003', '7187857775790532017', '7443719087125842801', '7645911984184220', '7699832228014829098', '7841243182917584827', '7884864095362714252', '7963164622649700701', '8270353789757678742', '8327642950764455479', '8361099786029960001', '8591619015958520777', '8602546173423975198', '8709075875931704956', '8712359548387897239', '8745508602748182624', '8810270153654530174', '8817813866789201989', '8822172186608861043', '882885791851492051', '8942982949436509216', '9000096877002249906', '9049746630789262332', '92368986368207275', '955730300479322378', '956047178382192543'} |         null
    
    (1 rows)
    