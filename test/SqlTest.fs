//----------------------------------------------------------------------------
//
// Copyright (c) 2013 The Tranq Team. 
//
// This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
// copy of the license can be found in the License.txt file at the root of this distribution. 
// By using this source code in any fashion, you are agreeing to be bound 
// by the terms of the Apache License, Version 2.0.
//
// You must not remove this notice, or any other, from this software.
//----------------------------------------------------------------------------

namespace Tranq.Test

module SqlTest = 

  open System
  open System.Collections.Generic
  open System.Data
  open System.Globalization
  open Microsoft.FSharp.Text.Lexing
  open NUnit.Framework
  open Tranq
  open Tranq.SqlAst
  open TestTool

  let private (|SingleNode|_|) statement = 
    match statement with Statement ([h]) -> Some h | _ -> None

  [<Test>]
  let ``parse Set : union`` () =
    let test s = 
      match Sql.parse s with Set _ -> () | x -> fail x
    test "select 1 union select 2" 
    test "select 1 union all select 2" 
    test "select 1 UNION select 2"
    test "select 1 UNION ALL select 2"

  [<Test>]
  let ``parse Set : union : subquery`` () =
    let test s = 
      match Sql.parse s with SingleNode (Select _) -> () | x -> fail x
    test "select 1 from (select 2 union select 3)" 

  [<Test>]
  let ``parse Set : minus`` () =
    let test s =
      match Sql.parse s with Set _ -> () | x -> fail x
    test "select 1 minus select 2"
    test "select 1 minus all select 2"
    test "select 1 MINUS select 2"
    test "select 1 MINUS ALL select 2"

  [<Test>]
  let ``parse Set : except`` () =
    let test sql = 
      match Sql.parse sql with Set _ -> () | x -> fail x
    test "select 1 except select 2" 
    test "select 1 except all select 2"
    test "select 1 EXCEPT select 2"
    test "select 1 EXCEPT ALL select 2"

  [<Test>]
  let ``parse Set : intersect`` () =
    let test sql = 
      match Sql.parse sql with Set _ -> () | x -> fail x
    test "select 1 intersect select 2"
    test "select 1 intersect all select 2"
    test "select 1 INTERSECT select 2" 
    test "select 1 INTERSECT ALL select 2"

  [<Test>]
  let ``parse Select`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Select _) -> () | x -> fail x
    test "select" 
    test "SELECT" 
    test "sElEcT"

  [<Test>]
  let ``parse From`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (From _) -> () | x -> fail x
    test "from"
    test "FROM"
    test "fRoM"

  [<Test>]
  let ``parse Where`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Where _) -> () | x -> fail x
    test "where"
    test "WHERE"
    test "wHeRe"

  [<Test>]
  let ``parse Having`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Having _) -> () | x -> fail x
    test "having"
    test "HAVING"
    test "hAvInG"

  [<Test>]
  let ``parse GroupBy`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (GroupBy _) -> () | x -> fail x
    test "group by" 
    test "GROUP BY" 
    test "gRoUp   By"

  [<Test>]
  let ``parse OrderBy`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (OrderBy _) -> () | x -> fail x
    test "order by" 
    test "ORDER BY" 
    test "oRdEr   By"

  [<Test>]
  let ``parse ForUpdate`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (ForUpdate _) -> () | x -> fail x
    test "for update" 
    test "FOR UPDATE" 
    test "fOr  UpDaTe"

  [<Test>]
  let ``parse And`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (And _) -> () | x -> fail x
    test "and" 
    test "AND" 
    test "aNd" 

  [<Test>]
  let ``parse Or`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Or _) -> () | x -> fail x
    test "or" 
    test "OR" 
    test "oR" 

  [<Test>]
  let ``parse At`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (BindVarComment _) -> () | x -> fail x
    test "@name" 

  [<Test>]
  let ``parse Parens`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Parens _) -> () | x -> fail x
    test "()" 
    test "( )"
    test "(select)"

  [<Test>]
  let ``parse IfBlock : if`` () =
    let test sql expr = 
      match Sql.parse sql with 
      | SingleNode (IfBlock (IfComment (x, _, _), _, _, _)) when x = expr -> () 
      | x -> fail x
    test "/*%if aaa *//*%end */" "aaa"
    test "/*% if aaa *//*%end */" "aaa"
    test "/*% \n if aaa *//*%end */" "aaa"

  [<Test>]
  let ``parse IfBlock : single elif`` () =
    let test sql expr = 
      match Sql.parse sql with 
      | SingleNode (IfBlock (_, elifCommentList, _, _)) ->
        match elifCommentList with
        | [ElifComment (x, _, _)] when x = expr -> ()
        | x -> fail x
      | x -> fail x
    test "/*%if aaa *//*%elif bbb *//*%end */" "bbb"
    test "/*%if aaa *//*% elif bbb *//*%end */" "bbb"
    test "/*%if aaa *//*% \n elif bbb *//*%end */" "bbb"

  [<Test>]
  let ``parse IfBlock : multiple elif`` () =
    let test sql expr1 expr2 = 
      match Sql.parse sql with 
      | SingleNode (IfBlock (_, elifCommentList, _, _)) ->
        match elifCommentList with
        | [ElifComment (x, _, _); ElifComment (y, _, _)] when x = expr1 && y = expr2 -> () 
        | x -> fail x
      | x -> fail x
    test "/*%if aaa *//*%elif bbb *//*%elif ccc *//*%end */" "bbb" "ccc"

  [<Test>]
  let ``parse IfBlock : else`` () =
    let test sql = 
      match Sql.parse sql with 
      | SingleNode (IfBlock (_, _, Some (ElseComment _), _)) -> () 
      | x -> fail x
    test "/*%if aaa *//*%else*//*%end */"
    test "/*%if aaa *//*% else*//*%end */"
    test "/*%if aaa *//*% \n else*//*%end */"

  [<Test>]
  let ``parse IfBlock : end`` () =
    let test sql = 
      match Sql.parse sql with 
      | SingleNode (IfBlock (_, _, _, _)) -> () 
      | x -> fail x
    test "/*%if aaa *//*%end */"
    test "/*%if aaa *//*% end */"
    test "/*%if aaa *//*% \n end */"

  [<Test>]
  let ``parse IfComment : if comment is not closed`` () =
    try
      Sql.parse "/*%if aaa" |> ignore
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2010" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``parse ForBlock : for`` () =
    let test sql = 
      match Sql.parse sql with 
      | SingleNode (ForBlock (ForComment _, _)) -> () 
      | x -> fail x
    test "/*%for aaa *//*%end */"
    test "/*% for aaa *//*%end */"
    test "/*% \n for aaa *//*%end */"

  [<Test>]
  let ``parse ForBlock : end`` () =
    let test sql = 
      match Sql.parse sql with 
      | SingleNode (ForBlock (_, _)) -> () 
      | x -> fail x
    test "/*%for aaa *//*%end */"
    test "/*%for aaa *//*% end */"
    test "/*%for aaa *//*% \n end */"

  [<Test>]
  let ``parse unknown expression directive`` () =
    try
      Sql.parse "/*%hoge aaa *//*%end */" |> ignore
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2019" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``parse Word`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Word _) -> () | x -> fail x
    test "abc" 
    test "\"abc\.\"def\"" 
    test "[abc].[def]" 

  [<Test>]
  let ``parse Other`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Other _) -> () | x -> fail x
    test "+"
    test "-"
    test "*"

  [<Test>]
  let ``parse BlockComment`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (BlockComment _) -> () | x -> fail x
    test "/** aaa */"
    test "/*+ aaa */"
    test "/*: aaa */"
    test "/** /* */"

  [<Test>]
  let ``parse BlockComment : block comment is not closed`` () =
    try
      Sql.parse "/** aaa" |> ignore
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2011" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``parse LineComment`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (LineComment _) -> () | x -> fail x
    test "-- aaa"
    test "-- aaa\n"

  [<Test>]
  let ``parse Literal`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Literal _) -> () | x -> fail x
    test "'aaa'"
    test "'aa''a'"
    test "N'あいうえお'"
    test "N'あい''うえお'"
    test "123"
    test "+123"
    test "-123"
    test ".123"
    test "123.456"
    test "123.456e"
    test "123.456E"
    test "123.456e10"
    test "date  '2011-01-23'"
    test "date'2011-01-23'"
    test "DATE  '2011-01-23'"
    test "DATE'2011-01-23'"
    test "time '12:34:56'"
    test "time'12:34:56'"
    test "TIME '12:34:56'"
    test "TIME'12:34:56'"
    test "timestamp '2011-01-23 12:34:56'"
    test "timestamp'2011-01-23 12:34:56'"
    test "TIMESTAMP '2011-01-23 12:34:56'"
    test "TIMESTAMP'2011-01-23 12:34:56'"
    test "true"
    test "TRUE"
    test "false"
    test "FALSE"
    test "null"
    test "NULL"

  [<Test>]
  let ``parse Literal : single quote is not closed`` () =
    try
      Sql.parse "'aaa" |> ignore
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2012" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``parse BindVarComment`` () =
    let test sql expr = 
      match Sql.parse sql with SingleNode (BindVarComment (x, _, _)) when x = expr -> () | x -> fail x
    test "/* aaa */'bbb'" "aaa"
    test "/*aaa*/'bbb'" "aaa"

  [<Test>]
  let ``parse BindVarsComment`` () =
    let test sql expr = 
      match Sql.parse sql with SingleNode (BindVarsComment (x, _, _)) when x = expr -> () | x -> fail x
    test "/* aaa */('bbb')" "aaa"
    test "/*aaa*/('bbb')" "aaa"

  [<Test>]
  let ``parse EmbeddedVarComment`` () =
    let test sql expr = 
      match Sql.parse sql with SingleNode (EmbeddedVarComment (x, _)) when x = expr -> () | x -> fail x
    test "/*# aaa */" "aaa"
    test "/*#aaa*/" "aaa"

  [<Test>]
  let ``parse Statement`` () =
    let test sql = 
      match Sql.parse sql with Statement _ -> () | x -> fail x
    test "select * from aaa where bbb = /* ccc */'ddd'"

  [<Test>]
  let ``parse Whitespaces`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Whitespaces _) -> () | x -> fail x
    test " "
    test "    "
    test "\t"
    test "\t\t"
    test "  \t"

  [<Test>]
  let ``parse Newline`` () =
    let test sql = 
      match Sql.parse sql with SingleNode (Newline _) -> () | x -> fail x
    test "\n"
    test "\r\n"

  let dialect = MsSqlDialect()

  [<Test>]
  let ``prepare : simple select`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb = 1" []
    assert_equal "select * from aaa where bbb = 1" ps.Text
    
  [<Test>]
  let ``prepare : sub select`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where bbb = /* bbb */'hoge' and ccc = (select ddd from eee where fff = /* fff */'') and ggg = /* ggg */''"
        [Param("bbb", "0", typeof<string>)
         Param("fff", "1", typeof<string>)
         Param("ggg", "2", typeof<string>)]
    assert_equal "select * from aaa where bbb = @p0 and ccc = (select ddd from eee where fff = @p1) and ggg = @p2" ps.Text

  [<Test>]
  let ``prepare : bind var`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb = /*bbb*/1" [Param("bbb", 1, typeof<int>)]
    assert_equal "select * from aaa where bbb = @p0" ps.Text

  [<Test>]
  let ``prepare : bind var : no test literal right after bind var`` () =
    try
      Sql.prepare dialect "select * from aaa where bbb = /*bbb*/ 0 and 1 = 2" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2008" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``prepare : bind var : no test literal`` () =
    try
      Sql.prepare dialect "select * from aaa where bbb = /*bbb*/ and 1 = 2" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2008" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``prepare : bind var : no test literal before EOF`` () =
    try
      Sql.prepare dialect "select * from aaa where bbb = /*bbb*/" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2009" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``prepare : bind var : not found`` () =
    try
      Sql.prepare dialect "select * from aaa where bbb = /*bbb*/1" [Param("aaa", 1, typeof<int>)] |> ignore
      fail ()
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2007" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``prepare : bind vars`` () =
    let list = [10; 20; 30]
    let ps = Sql.prepare dialect "select * from aaa where bbb in /*bbb*/(1,2,3)" [Param("bbb", list, typeof<int list>)]
    assert_equal "select * from aaa where bbb in (@p0, @p1, @p2)" ps.Text
    
  [<Test>]
  let ``prepare : embedded var`` () =
    let ps = Sql.prepare dialect "select * from aaa /*# 'order by bbb' */" []
    assert_equal "select * from aaa order by bbb" ps.Text

  [<Test>]
  let ``prepare : top : where`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select top (/*top*/0) * from aaa where /*% if true */bbb = /*bbb*/1 /*% end */" 
       [Param("bbb", 1, typeof<int>); Param("top", 10, typeof<int>)]
    assert_equal "select top (@p0) * from aaa where bbb = @p1" ps.Text

  [<Test>]
  let ``prepare IfComment where left`` () =
    let ps = Sql.prepare dialect "select * from aaa where /*% if true */bbb = /*bbb*/1 /*% end */" [Param("bbb", 1, typeof<int>)]
    assert_equal "select * from aaa where bbb = @p0" ps.Text

  [<Test>]
  let ``prepare IfComment : where removed`` () =
    let ps = Sql.prepare dialect "select * from aaa where /*% if false */bbb = /*bbb*/1 /*% end */" [Param("bbb", 1, typeof<int>)]
    assert_equal "select * from aaa" ps.Text

  [<Test>]
  let ``prepare IfComment : where left : and left`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where /*% if true */bbb = /*bbb*/1 /*% end *//*% if true */and ccc = /*ccc*/1 /*% end */ " 
        [Param("bbb", 1, typeof<int>); Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where bbb = @p0 and ccc = @p1" ps.Text

  [<Test>]
  let ``prepare IfComment : where removed : and removed`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where /*% if false */bbb = /*bbb*/1 /*% end *//*% if true */and ccc = /*ccc*/1 /*% end */ " 
        [Param("bbb", 1, typeof<int>); Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where  ccc = @p0" ps.Text

  [<Test>]
  let ``prepare IfComment : parens removed : and removed`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where (/*% if false */bbb = /*bbb*/1 /*% end */) and ccc = /*ccc*/1" 
        [Param("bbb", 1, typeof<int>); Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where   ccc = @p0" ps.Text

  [<Test>]
  let ``prepare IfComment : parens left : and left`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where (/*% if true */bbb = /*bbb*/1 /*% end */) /*% if true */and ccc = /*ccc*/1 /*% end */" 
        [Param("bbb", 1, typeof<int>); Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where (bbb = @p0 ) and ccc = @p1" ps.Text

  [<Test>]
  let ``prepare IfComment : empty parens left : and left`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where bbb = getdate() and ccc = /*ccc*/1" 
        [Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where bbb = getdate() and ccc = @p0" ps.Text

  [<Test>]
  let ``prepare IfComment : whitespace parens left : and left`` () =
    let ps = 
      Sql.prepare 
        dialect 
        "select * from aaa where bbb = getdate(   ) and ccc = /*ccc*/1" 
        [Param("ccc", 2, typeof<int>)]
    assert_equal "select * from aaa where bbb = getdate(   ) and ccc = @p0" ps.Text

  [<Test>]
  let ``prepare IfComment : end comment is missing`` () =
    try
      Sql.prepare dialect "select * from aaa where /*% if bbb <> null */bbb = /*bbb*/1 and 1 = 1" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ2005" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare ElseComment : if comment is missing`` () =
    try
      Sql.prepare dialect "select * from aaa where /*% else */bbb = /*bbb*/1 and 1 = 1" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ2004" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare ElifComment : if comment is missing`` () =
    try
      Sql.prepare dialect "select * from aaa where /*% elif false */bbb = /*bbb*/1 and 1 = 1"[Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ2004" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare IfBlock : isn't enclosed in a clause`` () =
    try
      Sql.prepare dialect "select * from aaa /*% if bbb <> null */ where bbb = /*bbb*/1 /*% end */and 1 = 1" [Param("bbb", 1, typeof<int>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ2015" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare union`` () =
    let ps = Sql.prepare dialect "select * from aaa union select * from bbb" []
    assert_equal "select * from aaa union select * from bbb" ps.Text

  [<Test>]
  let ``prepare ForComment`` () =
    let seq = [1; 2; 3]
    let ps = 
      Sql.prepare dialect "
        select * from aaa 
        where 
        /*% for bbb in seq */bbb = /*bbb*/1 
          /*% if bbb_has_next */
            /*# 'and' */
          /*% end */
        /*% end */
        " [Param("seq", seq, typeof<int list>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 3 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value
    assert_equal 2 ps.Params.[1].Value
    assert_equal 3 ps.Params.[2].Value

  [<Test>]
  let ``preparePaginate ForComment : embedded variable property access in the where clause`` () =
    let seq = ["1"; "22"; "333"]
    let ps = 
      Sql.preparePaginate dialect "
        select * from aaa 
        where 
        /*% for bbb in seq */bbb/*#bbb.Length*/ = /*bbb*/1 
          /*% if bbb_has_next */
            /*# 'and' */
          /*% end */
        /*% end */
        order by
          aaa.id
        " [Param("seq", seq, typeof<int list>)] 1L 10L
    printfn "%s" ps.Text
    let sql = "select * from ( select temp_.*, row_number() over( order by
          temp_.id
         ) as soma_rownumber_ from ( 
        select * from aaa 
        where 
        bbb1 = @p0 
          
            and
          
        bbb2 = @p1 
          
            and
          
        bbb3 = @p2 
          
        
        ) temp_ ) temp2_ where soma_rownumber_ > @p3 and soma_rownumber_ <= @p4"
    assert_equal sql ps.Text
    assert_equal 5 ps.Params.Length
    assert_equal "1" ps.Params.[0].Value
    assert_equal "22" ps.Params.[1].Value
    assert_equal "333" ps.Params.[2].Value
    assert_equal 1L ps.Params.[3].Value
    assert_equal 11L ps.Params.[4].Value

  [<Test>]
  let ``prepare ForComment : illegal expression`` () =
    let seq = [1; 2; 3]
    try
      Sql.prepare dialect "/*% for seq *//*% end */" [Param("seq", seq, typeof<int list>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ2014" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare ForBlock : isn't enclosed in a clause`` () =
    let seq = [1; 2; 3]
    try
      Sql.prepare dialect "
        select * from aaa 
        /*% for bbb in seq */bbb = /*bbb*/1 
        where
          /*% if bbb_has_next */
            /*# 'and' */
          /*% end */
        /*% end */
        " [Param("seq", seq, typeof<int list>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex -> 
      stdout.WriteLine ex.Message
      assert_equal "TRANQ2015" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``prepare : dialect root env : escape`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* escape bbb */'a' escape '$'" [Param("bbb", "x%x", typeof<string>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "x$%x" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : escape : null`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* escape bbb */'a' escape '$'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : escape : option`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* escape bbb */'a' escape '$'" [Param("bbb", Some "x%x", typeof<string option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "x$%x" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prefix`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* prefix bbb */'a' escape '$'" [Param("bbb", "x%x", typeof<string>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "x$%x%" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prefix : null`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* prefix bbb */'a' escape '$'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prefix : option`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* prefix bbb */'a' escape '$'" [Param("bbb", Some "x%x", typeof<string option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "x$%x%" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : infix`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* infix bbb */'a' escape '$'" [Param("bbb", "x%x", typeof<string>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "%x$%x%" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : infix : null`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* infix bbb */'a' escape '$'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : infix : option`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* infix bbb */'a' escape '$'" [Param("bbb", Some "x%x", typeof<string option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "%x$%x%" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : suffix`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* suffix bbb */'a' escape '$'" [Param("bbb", "x%x", typeof<string>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "%x$%x" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : suffix : null`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* suffix bbb */'a' escape '$'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : suffix : option`` () =
    let ps = Sql.prepare dialect "select * from aaa where bbb like /* suffix bbb */'a' escape '$'" [Param("bbb", Some "x%x", typeof<string option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal "%x$%x" ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : isNullOrEmpty`` () =
    let ps = Sql.prepare dialect "select * from aaa where /*% if not (isNullOrEmpty bbb) */ bbb = /* bbb */'a'/*% end*/" [Param("bbb", "", typeof<string>)]
    assert_equal "select * from aaa" ps.Text

  [<Test>]
  let ``prepare : dialect root env : isNullOrEmpty : null`` () =
    let ps = Sql.prepare dialect "select * from aaa where /*% if not (isNullOrEmpty bbb) */ bbb = /* bbb */'a'/*% end*/" [Param("bbb", null, typeof<obj>)]
    assert_equal "select * from aaa" ps.Text

  [<Test>]
  let ``prepare : dialect root env : isNullOrEmpty : option`` () =
    let ps = Sql.prepare dialect "select * from aaa where /*% if not (isNullOrEmpty bbb) */ bbb = /* bbb */'a'/*% end*/" [Param("bbb", Some "", typeof<string option>)]
    assert_equal "select * from aaa" ps.Text

  [<Test>]
  let ``prepare : dialect root env : date`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* date bbb */'2000-01-01 00:00:00'" [Param("bbb", bbb, typeof<DateTime>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 23)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : date : null`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* date bbb */'2000-01-01 00:00:00'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : date : option`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* date bbb */'2000-01-01 00:00:00'" [Param("bbb", Some bbb, typeof<DateTime option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 23)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : date : Nullable`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* date bbb */'2000-01-01 00:00:00'" [Param("bbb", Nullable bbb, typeof<Nullable<DateTime>>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 23)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : nextDate`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* nextDate bbb */'2000-01-01 00:00:00'" [Param("bbb", bbb, typeof<DateTime>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 24)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : nextDate : null`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* nextDate bbb */'2000-01-01 00:00:00'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : nextDate : option`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* nextDate bbb */'2000-01-01 00:00:00'" [Param("bbb", Some bbb, typeof<DateTime option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 24)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : nextDate : Nullable`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* nextDate bbb */'2000-01-01 00:00:00'" [Param("bbb", Nullable bbb, typeof<Nullable<DateTime>>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 24)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prevDate`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* prevDate bbb */'2000-01-01 00:00:00'" [Param("bbb", bbb, typeof<DateTime>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 22)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prevDate : null`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* prevDate bbb */'2000-01-01 00:00:00'" [Param("bbb", null, typeof<obj>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prevDate : option`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* prevDate bbb */'2000-01-01 00:00:00'" [Param("bbb", Some bbb, typeof<DateTime option>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 22)) ps.Params.[0].Value

  [<Test>]
  let ``prepare : dialect root env : prevDate Nullable`` () =
    let bbb = DateTime(2011, 1, 23, 12, 13, 14)
    let ps = Sql.prepare dialect "select * from aaa where bbb > /* prevDate bbb */'2000-01-01 00:00:00'" [Param("bbb", Nullable bbb, typeof<Nullable<DateTime>>)]
    printfn "%s" ps.Text
    printfn "%A" ps.Params
    assert_equal 1 ps.Params.Length
    assert_equal (DateTime(2011, 1, 22)) ps.Params.[0].Value

  [<Test>]
  let resolveOrderByEmbeddedVariables () =
    let sql = "select * from xxx.aaa where xxx.aaa.bbb = /* bbb */'a' and xxx.aaa.ddd = /*#ddd*/ order by /*#orderby*/"
    let statement = dialect.ParseSql sql
    let sql = Sql.resolveOrderByEmbeddedVariables sql (dict [("orderby", (box "xxx.aaa.ccc", typeof<string>)); ("ddd", (box 1, typeof<int>))]) statement
    assert_equal "select * from xxx.aaa where xxx.aaa.bbb = /* bbb */'a' and xxx.aaa.ddd = /*# ddd */ order by xxx.aaa.ccc" sql

  type Hoge1 = {[<Id>]Id:int; Name:string; [<Version>]Version:int; }
  type Hoge2 = {[<Id>]Id:int; Name:string }
  type Hoge3 = {Name:string; [<Version>]Version:int}
  type Hoge4 = {Name:string}

  [<Test>]
  let ``prepareInsert : id and version`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let ps = Sql.prepareInsert dialect { Hoge1.Id = 1; Name = "aaa" ; Version = 2; } meta (InsertOpt())
    assert_equal "insert into Hoge1 ( Id, Name, Version ) values ( @p0, @p1, @p2 )" ps.Text
    assert_equal 3 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value
    assert_equal "aaa" ps.Params.[1].Value
    assert_equal 2 ps.Params.[2].Value

  [<Test>]
  let ``prepareInsert : id`` () =
    let meta = EntityMeta.make typeof<Hoge2> dialect
    let ps = Sql.prepareInsert dialect { Hoge2.Id = 1; Name = "aaa" } meta (InsertOpt())
    assert_equal "insert into Hoge2 ( Id, Name ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value
    assert_equal "aaa" ps.Params.[1].Value

  [<Test>]
  let ``prepareInsert : version`` () =
    let meta = EntityMeta.make typeof<Hoge3> dialect
    let ps = Sql.prepareInsert dialect { Hoge3.Name = "aaa"; Version = 2; } meta (InsertOpt())
    assert_equal "insert into Hoge3 ( Name, Version ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 2 ps.Params.[1].Value

  [<Test>]
  let ``prepareInsert`` () =
    let meta = EntityMeta.make typeof<Hoge4> dialect
    let ps = Sql.prepareInsert dialect { Hoge4.Name = "aaa" } meta (InsertOpt())
    assert_equal "insert into Hoge4 ( Name ) values ( @p0 )" ps.Text
    assert_equal 1 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value

  type Hoge5 = {[<Id(IdKind.Identity)>]Id:int; Name:string option; Age:int option; Salary:decimal option; [<Version>]Version:int; }

  [<Test>]
  let ``prepareInsert : exclude null`` () =
    let meta = EntityMeta.make typeof<Hoge5> dialect
    let ps = Sql.prepareInsert dialect { Hoge5.Id = 1; Name = Some "aaa"; Age = None; Salary = None; Version= 10 } meta (InsertOpt(ExcludeNull = true))
    assert_equal "insert into Hoge5 ( Name, Version ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 10 ps.Params.[1].Value

  [<Test>]
  let ``prepareInsert : exclude`` () =
    let meta = EntityMeta.make typeof<Hoge5> dialect
    let ps = Sql.prepareInsert dialect { Hoge5.Id = 1; Name = Some "aaa"; Age = Some 20; Salary = Some 100M; Version= 10 } meta (InsertOpt(Exclude = ["Name"; "Salary"]))
    assert_equal "insert into Hoge5 ( Age, Version ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal 20 ps.Params.[0].Value
    assert_equal 10 ps.Params.[1].Value

  [<Test>]
  let ``prepareInsert : include`` () =
    let meta = EntityMeta.make typeof<Hoge5> dialect
    let ps = Sql.prepareInsert dialect { Hoge5.Id = 1; Name = Some "aaa"; Age = Some 20; Salary = Some 100M; Version= 10 } meta (InsertOpt(Include = ["Age"]))
    assert_equal "insert into Hoge5 ( Age, Version ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal 20 ps.Params.[0].Value
    assert_equal 10 ps.Params.[1].Value

  [<Test>]
  let ``prepareInsert : exclude and include`` () =
    let meta = EntityMeta.make typeof<Hoge5> dialect
    let ps = 
      Sql.prepareInsert 
        dialect 
        { Hoge5.Id = 1; Name = Some "aaa"; Age = Some 20; Salary = Some 100M; Version= 10 } 
        meta 
        (InsertOpt(Exclude = ["Salary"], Include = ["Age"; "Salary"]))
    assert_equal "insert into Hoge5 ( Age, Version ) values ( @p0, @p1 )" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal 20 ps.Params.[0].Value
    assert_equal 10 ps.Params.[1].Value

  type Hoge7 = {[<Id(IdKind.Identity)>]Id:int; Name:string option; [<Version(VersionKind.Computed)>]Version:int; }

  [<Test>]
  let ``prepareInsert : NoInsertablePropertyException `` () =
    let meta = EntityMeta.make typeof<Hoge7> dialect
    try
      Sql.prepareInsert 
        dialect 
        { Hoge7.Id = 0; Name = None; Version= 0 } 
        meta 
        (InsertOpt(ExcludeNull = true)) |> ignore
      fail ()
    with
    | :? SqlException as ex when ex.MessageId = "TRANQ4024"->
      printfn "%A" ex
    | _ -> 
      fail ()

  [<Test>]
  let ``prepareUpdate id and version`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let ps = Sql.prepareUpdate dialect { Hoge1.Id = 1; Version = 2; Name = "aaa" } meta (UpdateOpt())
    assert_equal "update Hoge1 set Name = @p0, Version = Version + 1 where Id = @p1 and Version = @p2" ps.Text
    assert_equal 3 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value
    assert_equal 2 ps.Params.[2].Value

  [<Test>]
  let ``prepareUpdate id and version : ignore version`` () =
    let meta = EntityMeta.make typeof<Hoge1>dialect
    let ps = Sql.prepareUpdate dialect { Hoge1.Id = 1; Version = 2; Name = "aaa" } meta (UpdateOpt(IgnoreVersion = true))
    assert_equal "update Hoge1 set Name = @p0 where Id = @p1" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value

  [<Test>]
  let ``prepareUpdate id `` () =
    let meta = EntityMeta.make typeof<Hoge2> dialect
    let ps = Sql.prepareUpdate dialect { Hoge2.Id = 1; Name = "aaa" } meta (UpdateOpt())
    assert_equal "update Hoge2 set Name = @p0 where Id = @p1" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value

  type Hoge6 = {[<Id(IdKind.Identity)>]Id:int; Name:string option; Age:int option; Salary:decimal option; [<Version>]Version:int; }

  [<Test>]
  let ``prepareUpdate : eclude null `` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    let ps = Sql.prepareUpdate dialect { Hoge6.Id = 1; Name = Some "aaa"; Age = None; Salary = None; Version= 10 } meta (UpdateOpt(ExcludeNull = true))
    assert_equal "update Hoge6 set Name = @p0, Version = Version + 1 where Id = @p1 and Version = @p2" ps.Text
    assert_equal 3 ps.Params.Length
    assert_equal "aaa" ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value
    assert_equal 10 ps.Params.[2].Value

  [<Test>]
  let ``prepareUpdate : eclude `` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    let ps = Sql.prepareUpdate dialect { Hoge6.Id = 1; Name = Some "aaa"; Age = None; Salary = None; Version= 10 } meta (UpdateOpt(Exclude = ["Name"; "Salary"]))
    assert_equal "update Hoge6 set Age = @p0, Version = Version + 1 where Id = @p1 and Version = @p2" ps.Text
    assert_equal 3 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value
    assert_equal 10 ps.Params.[2].Value

  [<Test>]
  let ``prepareUpdate : include `` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    let ps = Sql.prepareUpdate dialect { Hoge6.Id = 1; Name = Some "aaa"; Age = None; Salary = Some 100M; Version= 10 } meta (UpdateOpt(Include = ["Age"; "Salary"]))
    assert_equal "update Hoge6 set Age = @p0, Salary = @p1, Version = Version + 1 where Id = @p2 and Version = @p3" ps.Text
    assert_equal 4 ps.Params.Length
    assert_equal DBNull.Value ps.Params.[0].Value
    assert_equal 100M ps.Params.[1].Value
    assert_equal 1 ps.Params.[2].Value
    assert_equal 10 ps.Params.[3].Value

  [<Test>]
  let ``prepareUpdate : include and exclude `` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    let ps = 
      Sql.prepareUpdate 
        dialect 
        { Hoge6.Id = 1; Name = Some "aaa"; Age = Some 20; Salary = Some 100M; Version= 10 } 
        meta 
        (UpdateOpt(Exclude = ["Salary"], Include = ["Age"; "Salary"]))
    assert_equal "update Hoge6 set Age = @p0, Version = Version + 1 where Id = @p1 and Version = @p2" ps.Text
    assert_equal 3 ps.Params.Length
    assert_equal 20 ps.Params.[0].Value
    assert_equal 1 ps.Params.[1].Value
    assert_equal 10 ps.Params.[2].Value

  [<Test>]
  let ``prepareUpdate : NoUpdatablePropertyException `` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    try
      Sql.prepareUpdate 
        dialect 
        { Hoge6.Id = 1; Name = Some "aaa"; Age = Some 20; Salary = Some 100M; Version= 10 } 
        meta 
        (UpdateOpt(Exclude = ["Name"; "Age"; "Salary"], IgnoreVersion = true)) |> ignore
      fail ()
    with 
    | :? SqlException as ex when ex.MessageId = "TRANQ4025"->
      printfn "%A" ex.Message
    | _ -> 
      fail ()

  [<Test>]
  let ``prepareDelete : id and version`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let ps = Sql.prepareDelete dialect { Hoge1.Id = 1; Version = 2; Name = "aaa" } meta (DeleteOpt())
    assert_equal "delete from Hoge1 where Id = @p0 and Version = @p1" ps.Text
    assert_equal 2 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value
    assert_equal 2 ps.Params.[1].Value

  [<Test>]
  let ``prepareDelete : id and version : ignore version`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let ps = Sql.prepareDelete dialect { Hoge1.Id = 1; Version = 2; Name = "aaa" } meta (DeleteOpt(IgnoreVersion = true))
    assert_equal "delete from Hoge1 where Id = @p0" ps.Text
    assert_equal 1 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value

  [<Test>]
  let ``prepareDelete : id`` () =
    let meta = EntityMeta.make typeof<Hoge2> dialect
    let ps = Sql.prepareDelete dialect { Hoge2.Id = 1; Name = "aaa" } meta (DeleteOpt())
    assert_equal "delete from Hoge2 where Id = @p0" ps.Text
    assert_equal 1 ps.Params.Length
    assert_equal 1 ps.Params.[0].Value

  type Color =
    | White = 0
    | Red = 1
    | Green = 2
    | Blue = 3

  [<Flags>]
  type FileAttributes =
    | ReadOnly = 0x0001
    | Hidden = 0x0002
    | System = 0x0004

  [<Test>]
  let ``MsSqlDialect : ConvertFromDbToClr : basic type`` () =
    let dialect = MsSqlDialect()
    // raw type
    assert_equal 1 (dialect.ConvertFromDbToClr(1, typeof<int>, null, null))
    assert_equal null (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<int>, null, null))
    // option type
    assert_equal (Some 1) (dialect.ConvertFromDbToClr(1, typeof<int option>, null, null))
    assert_equal None (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<int option>, null, null))

  [<Test>]
  let ``MsSqlDialect : ConvertFromDbToClr : enum type`` () =
    let dialect = MsSqlDialect()
    // raw type
    assert_equal Color.Green (dialect.ConvertFromDbToClr(2, typeof<Color>, null, null))
    assert_equal null (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<Color>, null, null))
    assert_equal FileAttributes.Hidden (dialect.ConvertFromDbToClr(0x0002, typeof<FileAttributes>, null, null))
    assert_equal (FileAttributes.ReadOnly ||| FileAttributes.Hidden) (dialect.ConvertFromDbToClr(0x0003, typeof<FileAttributes>, null, null))
    assert_equal null (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<FileAttributes>, null, null))
    // option type
    assert_equal (Some Color.Green) (dialect.ConvertFromDbToClr(2, typeof<Color option>, null, null))
    assert_equal None (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<Color option>, null, null))
    assert_equal (Some FileAttributes.Hidden) (dialect.ConvertFromDbToClr(0x0002, typeof<FileAttributes option>, null, null))
    assert_equal (Some (FileAttributes.ReadOnly ||| FileAttributes.Hidden)) (dialect.ConvertFromDbToClr(0x0003, typeof<FileAttributes option>, null, null))
    assert_equal None (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<FileAttributes option>, null, null))

  type Num = Num of int

  [<Test>]
  let ``MsSqlDialect : ConvertFromDbToClr : conv`` () =
    let repo = DataConvRepo()
    repo.Add({new IDataConv<Num, int> with 
      member this.Compose(v) = Num v
      member this.Decompose(Num(v)) = v})
    let dialect = MsSqlDialect(repo)
    // raw type
    assert_equal (Num 1) (dialect.ConvertFromDbToClr(1, typeof<Num>, null, null))
    assert_equal null (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<Num>, null, null))
    // option type
    assert_equal (Some(Num 1)) (dialect.ConvertFromDbToClr(1, typeof<Num option>, null, null))
    assert_equal None (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<Num option>, null, null))

  type NumOpt = NumOpt of int option

  [<Test>]
  let ``MsSqlDialect : ConvertFromDbToClr : conv option`` () =
    let repo = DataConvRepo()
    repo.Add({new IDataConv<NumOpt, int option> with 
      member this.Compose(v) = NumOpt v
      member this.Decompose(NumOpt(v)) = v})
    let dialect = MsSqlDialect(repo)
    // raw type
    assert_equal (NumOpt(Some 1)) (dialect.ConvertFromDbToClr(1, typeof<NumOpt>, null, null))
    assert_equal (NumOpt None) (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<NumOpt>, null, null))
    // option type
    assert_equal (Some(NumOpt(Some 1))) (dialect.ConvertFromDbToClr(1, typeof<NumOpt option>, null, null))
    assert_equal None (dialect.ConvertFromDbToClr(Convert.DBNull, typeof<NumOpt option>, null, null))

  [<Test>]
  let ``MsSqlDialect : ConvertFromClrToDb : basic type`` () =
    let dialect = MsSqlDialect()
    // raw type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(1, typeof<int>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(null, typeof<int>, null))
    // option type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some 1, typeof<int option>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(None, typeof<int option>, null))

  [<Test>]
  let ``MsSqlDialect : ConvertFromClrToDb : enum type`` () =
    let dialect = MsSqlDialect()
    // raw type
    assert_equal (box 2, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Color.Green, typeof<Color>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(null, typeof<Color>, null))
    assert_equal (box 2, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(FileAttributes.Hidden, typeof<FileAttributes>, null))
    assert_equal (box 3, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(FileAttributes.ReadOnly ||| FileAttributes.Hidden, typeof<FileAttributes>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(null, typeof<FileAttributes>, null))
    // option type
    assert_equal (box 2, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some(Color.Green), typeof<Color option>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(None, typeof<Color option>, null))
    assert_equal (box 2, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some(FileAttributes.Hidden), typeof<FileAttributes option>, null))
    assert_equal (box 3, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some(FileAttributes.ReadOnly ||| FileAttributes.Hidden), typeof<FileAttributes option>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(None, typeof<FileAttributes option>, null))

  type Age = Age of int

  [<Test>]
  let ``MsSqlDialect : ConvertFromClrToDb : conv`` () =
    let repo = DataConvRepo()
    repo.Add({new IDataConv<Age, int> with 
      member this.Compose(v) = Age v
      member this.Decompose(Age(v)) = v})
    let dialect = MsSqlDialect(repo)
    // raw type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Age 1, typeof<Age>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(null, typeof<Age>, null))
    // option type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some(Age 1), typeof<Age option>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(None, typeof<Age option>, null))

  type AgeOpt = AgeOpt of int option

  [<Test>]
  let ``MsSqlDialect : ConvertFromClrToDb : conv opt`` () =
    let repo = DataConvRepo()
    repo.Add({new IDataConv<AgeOpt, int option> with 
      member this.Compose(v) = AgeOpt v
      member this.Decompose(AgeOpt(v)) = v})
    let dialect = MsSqlDialect(repo)
    // raw type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(AgeOpt(Some 1), typeof<AgeOpt>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(null, typeof<AgeOpt>, null))
    // option type
    assert_equal (box 1, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(Some(AgeOpt(Some 1)), typeof<AgeOpt option>, null))
    assert_equal (Convert.DBNull, typeof<int>, DbType.Int32) (dialect.ConvertFromClrToDb(None, typeof<AgeOpt option>, null))

  [<Test>]
  let ``MsSqlDialect : FormatAsSqlLiteral`` () =
    let dialect = MsSqlDialect()
    assert_equal "null" (dialect.FormatAsSqlLiteral(Convert.DBNull, typeof<int>, DbType.Int32))
    assert_equal "N'aaa'" (dialect.FormatAsSqlLiteral("aaa", typeof<string>, DbType.String))
    assert_equal "'12:34:56'" (dialect.FormatAsSqlLiteral(TimeSpan(12, 34, 56), typeof<TimeSpan>, DbType.Time))
    assert_equal "'1234-01-23'" (dialect.FormatAsSqlLiteral(DateTime(1234, 01, 23), typeof<DateTime>, DbType.Date))
    assert_equal "'1234-01-23 13:34:46.123'" (dialect.FormatAsSqlLiteral(DateTime(1234, 01, 23, 13, 34, 46, 123), typeof<DateTime>, DbType.DateTime))
    let datetime = DateTime.ParseExact("1234-01-23 13:34:46.1234567", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)
    assert_equal "'1234-01-23 13:34:46.1234567'" (dialect.FormatAsSqlLiteral(datetime, typeof<DateTime>, DbType.DateTime2))
    let dtOffset = DateTimeOffset(1234, 01, 23, 13, 34, 46, 123, TimeSpan(1, 23, 00))
    assert_equal "'1234-01-23 13:34:46 +01:23'" (dialect.FormatAsSqlLiteral(dtOffset, typeof<DateTimeOffset>, DbType.DateTimeOffset))
    assert_equal "/** binary value is not shown */null" (dialect.FormatAsSqlLiteral([| 0uy .. 5uy |], typeof<byte[]>, DbType.Binary))
    assert_equal "'0'" (dialect.FormatAsSqlLiteral(false, typeof<bool>, DbType.Boolean))
    assert_equal "'1'" (dialect.FormatAsSqlLiteral(true, typeof<bool>, DbType.Boolean))
    assert_equal "123" (dialect.FormatAsSqlLiteral(123L, typeof<int64>, DbType.Int64))

  [<Test>]
  let ``MsSqlDialect : CreateParameterName`` () =
    let dialect = MsSqlDialect()
    assert_equal "@p0" (dialect.CreateParameterName(0))
    assert_equal "@p1" (dialect.CreateParameterName(1))

  [<Test>]
  let ``MsSqlDialect : object expression`` () =
    let dialect =
      { new MsSqlDialect() with 
        member this.Env = Map.empty :> IDictionary<string, obj * Type> }
    ()

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is zero`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where bbb = /* bbb */'a' order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 0L, 10L)
    assert_equal 
      "select top (/* soma_limit */10) * from aaa where bbb = /* bbb */'a' order by ccc"
      sql
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is zero : if block`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where /*% if true */bbb = /* bbb */'a' /*% elif true */ bbb = 'b' /*% else */ bbb = 'c' /*% end */ order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 0L, 10L)
    assert_equal 
      "select top (/* soma_limit */10) * from aaa where /*% if true */bbb = /* bbb */'a' /*% elif true */ bbb = 'b' /*% else */ bbb = 'c' /*% end */ order by ccc"
      sql
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is zero : for block`` () =
    let dialect = MsSqlDialect()
    let sql = 
      "select * from aaa where
      /*%for b in bbb*/
      bbb = /* b */'b' 
      /*%if bbb_has_next*/ /*#'or'*/ /*%end*/
      /*%end*/
      order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 0L, 10L)
    assert_equal
      "select top (/* soma_limit */10) * from aaa where
      /*% for b in bbb */
      bbb = /* b */'b' 
      /*% if bbb_has_next */ /*# 'or' */ /*% end */
      /*% end */
      order by ccc"
      sql
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is not zero`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where bbb = /* bbb */'a' order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 5L, 10L)
    assert_equal 
      "select * from ( select temp_.*, row_number() over( order by ccc ) as soma_rownumber_ from ( select * from aaa where bbb = /* bbb */'a' ) temp_ ) temp2_ where soma_rownumber_ > /* soma_offset */5 and soma_rownumber_ <= /* soma_offset + soma_limit */15"
      sql
    assert_true (exprCtxt.ContainsKey "soma_offset")
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is not zero : column qualified`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from xxx.aaa where xxx.aaa.bbb = /* bbb */'a' order by xxx.aaa.ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 5L, 10L)
    assert_equal 
      "select * from ( select temp_.*, row_number() over( order by temp_.ccc ) as soma_rownumber_ from ( select * from xxx.aaa where xxx.aaa.bbb = /* bbb */'a' ) temp_ ) temp2_ where soma_rownumber_ > /* soma_offset */5 and soma_rownumber_ <= /* soma_offset + soma_limit */15"
      sql
    assert_true (exprCtxt.ContainsKey "soma_offset")
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is not zero : if block`` () =
    let dialect = MsSqlDialect()
    let sql =
      "select * from aaa where
      /*%for b in bbb*/
      bbb = /* b */'b' 
      /*%if bbb_has_next*/ /*#'or'*/ /*%end*/
      /*%end*/
      order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 5L, 10L)
    assert_equal
      "select * from ( select temp_.*, row_number() over( order by ccc ) as soma_rownumber_ from ( select * from aaa where
      /*% for b in bbb */
      bbb = /* b */'b' 
      /*% if bbb_has_next */ /*# 'or' */ /*% end */
      /*% end */
      ) temp_ ) temp2_ where soma_rownumber_ > /* soma_offset */5 and soma_rownumber_ <= /* soma_offset + soma_limit */15"
      sql
    assert_true (exprCtxt.ContainsKey "soma_offset")
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is not zero : for block`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where /*% if true */ bbb = /* bbb */'a' /*% end */ order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForPagination(statement, sql, Map.empty, 5L, 10L)
    assert_equal 
      "select * from ( select temp_.*, row_number() over( order by ccc ) as soma_rownumber_ from ( select * from aaa where /*% if true */ bbb = /* bbb */'a' /*% end */ ) temp_ ) temp2_ where soma_rownumber_ > /* soma_offset */5 and soma_rownumber_ <= /* soma_offset + soma_limit */15"
      sql
    assert_true (exprCtxt.ContainsKey "soma_offset")
    assert_true (exprCtxt.ContainsKey "soma_limit")

  [<Test>]
  let ``MsSqlDialect : RewriteForPagination : offset is not zero : no order by clause`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where bbb = /* bbb */'a'"
    let statement = Sql.parse sql
    try
      dialect.RewriteForPagination(statement, sql, Map.empty, 5L, 10L) |> ignore
      fail ()
    with
    | :? SqlException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ2016" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``MsSqlDialect : RewriteForCount`` () =
    let dialect = MsSqlDialect()
    let sql = "select * from aaa where bbb = /* bbb */'a' order by ccc"
    let statement = Sql.parse sql
    let sql, exprCtxt = dialect.RewriteForCount(statement, sql,  Map.empty) 
    assert_equal 
      "select count_big(*) from ( select * from aaa where bbb = /* bbb */'a'  ) t_"
      sql
    assert_true (exprCtxt.Count = 0)

  [<Test>]
  let ``MsSqlDialect : EscapeMetaChars`` () =
    let dialect = MsSqlDialect()
    assert_equal "abc" (dialect.EscapeMetaChars "abc")
    assert_equal "ab$%c" (dialect.EscapeMetaChars "ab%c")
    assert_equal "ab$_c" (dialect.EscapeMetaChars "ab_c")
    assert_equal "ab$$c" (dialect.EscapeMetaChars "ab$c")
    assert_equal "ab$[c" (dialect.EscapeMetaChars "ab[c")

  [<Test>]
  let ``OracleDialect : CreateParameterName`` () =
    let dialect = OracleDialect()
    let value = dialect.CreateParameterName("hoge")
    assert_equal "hoge" value

  [<Test>]
  let ``function : illegal argument type`` () =
    let dialect =
      { new MsSqlDialect() with 
        member this.Env =  
          let dict = new Dictionary<string, (obj * Type)>(base.Env) :> IDictionary<string, (obj * Type)>
          let hoge s = s + "hoge"
          dict.Add("hoge", (box hoge, hoge.GetType()))
          dict } :> IDialect
    try
      Sql.prepare dialect "select * from aaa where /*% if hoge(bbb) */ bbb = /* bbb */'a'/*% end*/" [Param("bbb", 10, typeof<int>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex->
      printf "%s" ex.Message
      assert_equal "TRANQ2007" ex.MessageId
      assert_true (ex.Message.Contains "[TRANQ1024]")

  [<Test>]
  let ``function : invocation failed`` () =
    let dialect =
      { new MsSqlDialect() with 
        member this.Env =  
          let dict = new Dictionary<string, (obj * Type)>(base.Env) :> IDictionary<string, (obj * Type)>
          let hoge s:string = raise <| invalidOp "hoge is invalid"
          dict.Add("hoge", (box hoge, hoge.GetType()))
          dict } :> IDialect
    try
      Sql.prepare dialect "select * from aaa where /*% if hoge(bbb) */ bbb = /* bbb */'a'/*% end*/" [Param("bbb", "foo", typeof<string>)] |> ignore
      fail ()
    with 
    | :? SqlException as ex->
      printf "%s" ex.Message
      assert_equal "TRANQ2007" ex.MessageId
      assert_true (ex.Message.Contains "[TRANQ1025]")

