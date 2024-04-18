// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func BenchmarkDetachCondAndBuildRangeForIndex(b *testing.B) {
	store := testkit.CreateMockStore(b)
	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("USE test")
	testKit.MustExec("DROP TABLE IF EXISTS t")
	testKit.MustExec(`
CREATE TABLE t (
    id bigint(20) NOT NULL,
    flag tinyint(1) NOT NULL,
    start_time bigint(20) NOT NULL,
    end_time bigint(20) NOT NULL,
    org_id bigint(20) NOT NULL,
    work_type varchar(20) CHARACTER SET latin1 DEFAULT NULL,
    KEY idx (work_type, flag, org_id, end_time)
)`)
	const longInListQuery = `
SELECT
    *
FROM
    test.t
WHERE
    flag = false
    AND work_type = 'ART'
    AND start_time < 1437233819011
    AND org_id IN (
        6914445279216,13370400597894,23573357446410,25887731612857,47914138043476,55587486395595,81718641401629,116900287316705,145561135354847,146149695520705,150737101640717,
        156461412195734,159282589442914,172009309011258,177247737846954,186431984199126,192702036012003,198028383844036,203822128217766,204361899227892,222307981616040,
        223676791310456,239710781952147,284504079630201,293577747630605,294033490212963,295616321229667,296595862564733,306758307660237,312453546107536,341725138386801,
        365564508529854,367823101074613,372136375994705,379986503803645,391811651805728,400703265358976,402101198418969,418909618067022,431707392441212,433127041410999,
        453871162499705,459423393488794,468411967568789,470295426508062,474431880357867,486212302560292,491505628336549,501258676986205,512885499269076,541562269064881,
        573472004072939,588733301257768,615894236361514,615983097695492,632401842501612,647142957465854,718424118160213,727429900786891,730535283550760,738198024914914,
        739163331366841,752647396960214,756654620675819,771528730062551,780002237988119,819197371932508,841300462416232,850412457085478,874244695288775,881570046324012,
        888197716219703,891639557310370,891974587948562,894056936863746,896673560435518,907536217395121,911688951561329,915825484067466,955508673412475,970655461426927,
        972378619997200,995940920336141,1001616070548723,1002328390838858,1008841511217759,1027157044666161,1048868628313964,1066571897813670,1077847212985966,1080168008312256,
        1096474319900512,1102806753403857,1117700255389200,1121170709282654,1160894105829436,1173396752247253,1174823990738043,1176756778119821,1183639496028232,1224377491431012,
        1240466389734677,1265076660322902,1269569491282720,1272581574031499,1294849094933502,1295099688484884,1298881176988511,1299992642087679,1307669929489952,1338074385647926,
        1342415156765380,1360218717881999,1377658957083475,1392922543432192,1407444098594050,1438256495838735,1445134088155147,1486929584894557,1494681055271250,1500940354882616,
        1530516421560327,1532590570974295,1544648947254643,1561923580960451,1563587565476473,1565067823935596,1573069534228327,1573167213450271,1573297960637648,1577324450386436,
        1595234199645128,1595320706602899,1595934401297767,1616085587597345,1652295812716667,1655495487920136,1663475672388133,1668352817492466,1681094348275341,1689623403182214,
        1701682724927093,1705012823832699,1710393138044599,1716535649128474,1733575964270463,1750190609804974,1754580077690816,1756061776687456,1758058273255859,1775158332937577,
        1786728287430927,1816461420376899,1828580334315536,1858008005313000,1878841219054602,1878932921719554,1904081347331116,1904820184794904,1913069596895373,1941380005857044,
        1954836070968071,1955618820347782,1983296066429676,1987819713385690,1998098943250021,2013403425656222,2026046763398088,2026621786756595,2042249014990205,2046741004470190,
        2054370107316514,2082578854015062,2087183461591329,2087192311688265,2095277921868705,2103249621417272,2105159282073449,2106385538583803,2115025577188264,2115892671475192,
        2144855844328126,2145526421460724,2155282047243675,2170620433275766,2189596848694026,2194468311513858,2213049505974092,2213713514793282,2213911591088204,2215904332099994,
        2220235232411590,2226449138693468,2281727415070895,2288760906462231,2292201263531973,2292800860074434,2305015986098173,2311091146951322,2345959699274993,2356192386220089,
        2358508761766515,2372253813770528,2372994254274287,2374279183118353,2382395547546489,2397593929551113,2414904392525386,2416320859150803,2432407185506251,2439133858011514,
        2456909988876966,2490057196713078,2491000908839300,2501843499019962,2539856169115632,2542691236689315,2554165079560216,2565016421484028,2579373020942520,2583310536030502,
        2601385761450563,2603243552830302,2609138752573551,2628285706286347,2631734058797888,2638342575912817,2640167419150776,2651402302096646,2652627728219962,2677347449018607,
        2680209147172480,2683108753662485,2697695717735514,2699481485241986,2706019556864874,2707225343321107,2707841703322085,2724535386459144,2751805187614069,2759827465036125,
        2761426575202406,2768997857008184,2780782950787373,2789872834409453,2801402100587551,2806075464399632,2811893029385689,2816433481597910,2817217705468440,2817327738406355,
        2834191662385443,2838661299874842,2841835162527294,2842790844846179,2873548708258327,2879581389559553,2881798195775557,2943715564248539,2951224334569493,2958397742216527,
        2962117760574537,2977264272542363,2982743447396379,3009349759215772,3010012117130847,3010096874529870,3010115852485268,3012535704934837,3019483201458596,3036314602733381,
        3036837746136599,3039514679368453,3052506678397436,3059152178913030,3061095276596365,3063233426258797,3085115838323303,3093308450248420,3101208987984055,3106059275305341,
        3107559394454149,3110050933644367,3138545183162445,3140678914029638,3142109724205254,3142527081183238,3160670144726081,3160829106050702,3172902656372574,3181996104936476,
        3185563316673259,3189751274857877,3211729550313903,3213599609783958,3213659878256809,3215918533522245,3225137047012900,3231023124763183,3237856608607317,3247879729114765,
        3253596244997212,3256221808530784,3259756319203329,3260467132021662,3270706364495298,3273179837457878,3281263986182695,3296686215236784,3315584830022847,3317781471030011,
        3338120669693067,3342373672540130,3348941534532661,3359715878023917,3383970565360566,3397868551281252,3428808017724584,3449238661063482,3450439330534765,3471286956110444,
        3472931370488350,3473088266701087,3474445823605231,3477506057124755,3477541077050002,3488929919781737,3502330373943603,3504844077112105,3508329172255490,3510690524473209,
        3512324709745663,3514007754921924,3558011694836438,3559330712604565,3562145253615132,3581148885399840,3591312468185522,3597256243542091,3618160287855458,3625686898155792,
        3650194338715713,3650610878210125,3668345904483321,3668494673430762,3677776463522008,3694233081600122,3709632134619598,3725419513943971,3730099951927002,3734961839633730,
        3788380331922768,3811246425255446,3815582365292463,3829203122180282,3847292141308076,3877507310017402,3882136043994493,3887872593644033,3891280433757250,3924114035682754,
        3930635798027692,3940634174809349,3955900287161214,3957844806020309,3973236219509940,3981294421878412,3984846206013660,3993802859865565,3996764828980278,4023461371356880,
        4030830455174294,4032434581680299,4041625011713530,4041957068079946,4058955264781991,4083680454731905,4095581705542542,4112420671677445,4121292361441010,4133226631387396,
        4139004365572538,4161250756754756,4163706760683594,4170471653379067,4171134267004311,4177796537235481,4180802682160942,4203191696595400,4209464578955045,4223422057959415,
        4237541104444937,4257691911774311,4260020795088571,4301574030764989,4301922471400280,4304478206038048,4314941265701364,4320330498423583,4339739390410992,4342413486284428,
        4347136230283432,4351145740656078,4358837874704787,4362622126951624,4364582223851552,4366497646764759,4385815379876479,4406431382404050,4409339407622327,4411432076559821,
        4449849757340102,4453892102562139,4466153465045159,4479272299804907,4486938493241801,4500505590495671,4502678993390350,4508608408879950,4515778758013390,4516708986545589,
        4534650929880461,4537213242644302,4537280160911816,4544757774741059,4544796825985803,4567299173366450,4586846032456054,4607174945068672,4613242210966642,4625035792839901,
        4634756593700865,4635123568273790,4659875963875224,4670322637424975,4683035442855866,4689033135620199,4714163851388857,4714214015125119,4744848633979578,4750937375207328,
        4776805289846989,4804031931975645,4837280540915990,4840957238353452,4868526553354967,4875063418864529,4909882543513230,4912268119820614,4919123728900332,4929754602909549,
        4941072993543698,4941174020949904,4948032640819331,4955057670206957,4989823480030237,4990195550280933,5011619499820208,5013143564325843,5016786248387969,5019292677276101,
        5045230878000223,5053158166772953,5058611677018883,5083565032770599,5100847523417394,5105223137691724,5116076462386020,5117104556161083,5137323839372187,5159591155175114,
        5174145884534911,5182009696238822,5207529188216676,5229363397364492,5231081768308950,5233170527517877,5238337785560206,5259149223152958,5259347545688076,5265284989590946,
        5288711160650011,5297955243309025,5300535720011230,5311590629866593,5313248950470856,5313960220028487,5321218075331795,5334031591400346,5340934779949776,5380276587818617,
        5380604989545853,5392427172834832,5419648071490001,5436430269421440,5438720576124743,5442272167466546,5443531545450195,5462404261617760,5484761325677647
    )
`
	sctx := testKit.Session().(sessionctx.Context)
	stmts, err := session.Parse(sctx, longInListQuery)
	require.NoError(b, err)
	require.Len(b, stmts, 1)
	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
	require.NoError(b, err)
	ctx := context.Background()
	p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
	require.NoError(b, err)
	selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
	tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
	require.NotNil(b, selection)
	conds := make([]expression.Expression, len(selection.Conditions))
	for i, cond := range selection.Conditions {
		conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
	}
	cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[0])
	require.NotNil(b, cols)

	b.ResetTimer()
	pctx := sctx.GetPlanCtx()
	for i := 0; i < b.N; i++ {
		_, err = ranger.DetachCondAndBuildRangeForIndex(pctx.GetRangerCtx(), conds, cols, lengths, 0)
		require.NoError(b, err)
	}
	b.StopTimer()
}
