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

namespace Tranq

open System
open System.Collections
open System.Collections.Generic
open System.Data
open System.Data.Common
open System.Globalization
open System.Text
open System.Text.RegularExpressions
open System.Reflection
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Text.Lexing
open Microsoft.FSharp.Text.Parsing
open Tranq
open Tranq.Text
open Tranq.SqlAst
open Tranq.SqlParser

type internal SqlException (message, ?innerException:exn) =
  inherit InvalidOperationException (Message.format message, match innerException with Some ex -> ex | _ -> null)
  member this.MessageId = message.Id

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SqlRewriteHelper =
  let writeIfComment ifComment (buf:StringBuilder) f = 
    match ifComment with
    | IfComment(expression, nodeList, _) ->
      buf.Append "/*% if " |> ignore
      buf.Append expression |> ignore
      buf.Append " */" |> ignore
      List.fold f buf nodeList

  let writeElifComment elifComment (buf:StringBuilder) f = 
    match elifComment with
    | ElifComment(expression, nodeList, _) ->
      buf.Append "/*% elif " |> ignore
      buf.Append expression |> ignore
      buf.Append " */" |> ignore
      List.fold f buf nodeList

  let writeElseComment elseComment (buf:StringBuilder) f = 
    match elseComment with
    | ElseComment(nodeList, _) ->
      buf.Append "/*% else */" |> ignore
      List.fold f buf nodeList
  
  let writeForComment forComment (buf:StringBuilder) f  = 
    match forComment with
    | ForComment(expression, nodeList, _) ->
      buf.Append "/*% for " |> ignore
      buf.Append expression |> ignore
      buf.Append " */" |> ignore
      List.fold f buf nodeList

  let writeIfBlock (ifComment, elifCommentList, elseComment, nodeList) (buf:StringBuilder) f  = 
    let buf = writeIfComment ifComment buf f
    let buf = List.fold (fun buf comment -> writeElifComment comment buf f) buf elifCommentList
    let buf =
      match elseComment with 
      | Some comment -> writeElseComment comment buf f
      | _ -> buf
    buf.Append "/*% end */" |> ignore
    List.fold f buf nodeList

  let writeForBlock (forComment, nodeList) (buf:StringBuilder) f =
    let (buf:StringBuilder) = writeForComment forComment buf f
    buf.Append "/*% end */" |> ignore
    List.fold f buf nodeList

  let writeBindVarComment (expression:string, node) (buf:StringBuilder) f = 
    buf.Append "/* " |> ignore
    buf.Append expression |> ignore
    buf.Append " */" |> ignore
    f buf node

  let writeEmbeddedVarComment (expression:string) (buf:StringBuilder) = 
    buf.Append "/*# " |> ignore
    buf.Append expression |> ignore
    buf.Append " */" |> ignore
    buf

  let writeParens statement (level:int ref) (buf:StringBuilder) f = 
    buf.Append "(" |> ignore
    incr level
    let (buf:StringBuilder) = f buf statement
    decr level
    buf.Append ")"

  let writeSet (set:string, lhs, rhs) (buf:StringBuilder) f = 
    let buf:StringBuilder = f buf lhs
    buf.Append set |> ignore
    f buf rhs

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Sql =

  type internal State (dialect, env: IDictionary<string, (obj * Type)>, paramIndex) =
    let buf = SqlBuilder(dialect, ParamIndex = paramIndex)
    member this.Dialect = dialect
    member this.Sql = buf.Sql
    member this.FormattedSql = buf.FormattedSql
    member this.Params = buf.Params
    member this.ParamIndex = buf.ParamIndex
    member val Env = env with get, set
    member val IsAvailable = false with get, set
    member val IsInsideBlock = false with get, set
    member val StartsWithClause = false with get, set
    member this.AppendState (other : State) =
      buf.Sql.Append(other.Sql) |> ignore
      buf.FormattedSql.Append(other.FormattedSql) |> ignore
      other.Params |> Seq.iter (fun p -> buf.Params.Add(p))
      buf.ParamIndex <- other.ParamIndex
    member this.AppendFragment (fragment : string) =
      buf.Append(fragment)
    member this.AppendFragment (fragment : string, hint : Node) =
      if not this.StartsWithClause && buf.Sql.ToString().Trim().Length = 0 then
        match hint with
        | Select _ 
        | From _ 
        | Where _ 
        | Having _ 
        | GroupBy _ 
        | OrderBy _ 
        | ForUpdate _ -> this.StartsWithClause <- true
        | _ -> ()
      buf.Append(fragment)
    member this.Bind (value, typ) =
      buf.Bind(value, typ)
    member this.Build() =
      buf.Build()

  let internal startLocation = { pos_bol = 0; pos_fname = "Sql"; pos_cnum = 0; pos_lnum = 1 }

  let internal getErrorMessage (ctxt:ParseErrorContext<token>) =
    match ctxt.CurrentToken with
    | None -> 
      SR.TRANQ2000 ctxt.Message
    | Some token -> 
      SR.TRANQ2004 (token_to_string token) 

  let internal getLocation (lexbuf:LexBuffer<char>) =
    let pos = lexbuf.StartPos
    { Location.pos_bol = pos.pos_bol
      pos_fname= pos.pos_fname
      pos_cnum = pos.pos_cnum
      pos_lnum = pos.pos_lnum }

  let internal appendDetail {Id = id; Text = text} (sql:string) loc = 
    { Id = id; Text = text + " " + (SR.TRANQ2006(sql, loc.pos_lnum, loc.pos_cnum)).Text }

  let internal appendSimpleDetail {Id = id; Text = text} (sql:string) = 
    { Id = id; Text = text + " " + (SR.TRANQ2018(sql)).Text }

  let parse sql =
    let lexbuf = LexBuffer<char>.FromString(sql)
    let loc = startLocation
    lexbuf.EndPos <- { 
      pos_bol = loc.pos_bol
      pos_fname = loc.pos_fname
      pos_cnum = loc.pos_cnum
      pos_lnum = loc.pos_lnum } 
    try
      SqlParser.start SqlLexer.tokenize lexbuf
    with 
    | SqlParseError (message, loc) -> 
      let message = appendDetail message sql loc
      raise <| SqlException (message)
    | SqlParseErrorWithContext obj ->
      let ctxt = obj :?> ParseErrorContext<token>
      let message = getErrorMessage ctxt
      let loc = getLocation lexbuf
      let message = appendDetail message sql loc
      raise <| SqlException (message) 
    | ex -> 
      let p = lexbuf.EndPos
      let loc = { 
        Location.pos_bol = p.pos_bol
        pos_fname= p.pos_fname
        pos_cnum = p.pos_cnum
        pos_lnum = p.pos_lnum }
      let message = appendDetail (SR.TRANQ2001 ()) sql loc
      raise <| SqlException (message, ex)

  let internal concatEnv (x:IDictionary<string, obj * Type>) (y:IDictionary<string, obj * Type>) = 
    let env = new Dictionary<string, obj * Type>(x) :> IDictionary<string, obj * Type>
    y |> Seq.iter (fun (KeyValue(key, value)) ->
      env.Remove(key) |> ignore
      env.Add(key, value) )
    env
  
  let internal (|If|_|) ifComment eval _ = 
    match ifComment with 
    | IfComment(expr, nodes, loc) -> 
      match eval expr loc with 
      | true -> Some nodes 
      | _ -> None 

  let internal (|Elif|_|) elifCommentList eval _ =
    List.tryPick (function 
      | ElifComment(expr, nodes, loc) -> 
        match eval expr loc with 
        | true -> Some nodes
        | _ -> None) elifCommentList
  
  let internal (|Else|_|) elseComment _ =
    match elseComment with 
    | Some (ElseComment (nodes, _)) -> Some nodes 
    | _ -> None

  let internal evaluate env expr loc sql =
    try
      Expr.evaluate expr env
    with 
    | :? ExprException as ex ->
      let message = appendDetail (SR.TRANQ2007 ()) sql loc
      let message = Message.appendText message ex.Message
      match ex.InnerException with
      | null -> raise <| SqlException (message)
      | _ -> raise <| SqlException (message, ex)

  let internal invalidBlock keyword loc sql =
    let message = appendDetail (SR.TRANQ2015 (keyword)) sql loc
    raise <| SqlException (message)

  let internal (|EmptyStatement|_|) =
    function 
    | Statement(nodes) ->
      match nodes with
      | [] -> Some ""
      | [Whitespaces(s)] -> Some s
      | _ -> None
    |_ -> None

  let internal bindVars (state : State) (ie:IEnumerable) =
    let parameters = ie |> Seq.cast<obj> |> Seq.toList
    if not parameters.IsEmpty then
      state.AppendFragment "("
      state.Bind(parameters.Head, (parameters.Head.GetType()))
      Seq.iter (fun p -> 
        state.AppendFragment ", "
        state.Bind (p, p.GetType())) parameters.Tail
      state.AppendFragment(")")

  let internal generate initState sql statement =
    let eval env expr loc =
      evaluate env expr loc sql
    let rec foldInsideBlock (state : State) nodeList = 
      let preserve = state.IsInsideBlock
      state.IsInsideBlock <- true
      let state = List.fold visitNode state nodeList
      state.IsInsideBlock <- preserve
      state
    and visitStatement state = 
      function 
      | Statement nodeList -> 
        List.fold visitNode state nodeList 
      | Set (set, lhs, rhs, _) ->
        let state = visitStatement state lhs
        state.AppendFragment(set)
        let state = visitStatement state rhs
        state
    and visitNode (state : State) =
      function
      | Word(fragment)
      | Other(fragment)
      | Literal(fragment) ->
        state.IsAvailable <- true
        state.AppendFragment fragment
        state
      | Whitespaces(fragment)
      | Newline(fragment) 
      | BlockComment(fragment)
      | LineComment(fragment) ->
        state.AppendFragment fragment
        state
      | Select(keyword, nodeList, loc)
      | From(keyword, nodeList, loc) as node ->
        if state.IsInsideBlock then 
          invalidBlock keyword loc sql
        state.AppendFragment(keyword, node)
        List.fold visitNode state nodeList
      | Where(keyword, nodeList, loc)
      | GroupBy(keyword, nodeList, loc)
      | Having(keyword, nodeList, loc)
      | OrderBy(keyword, nodeList, loc)
      | ForUpdate(keyword, nodeList, loc) as node->
        if state.IsInsideBlock then 
          invalidBlock keyword loc sql
        let childState = 
          List.fold visitNode (State(state.Dialect, state.Env, state.ParamIndex)) nodeList
        if childState.IsAvailable then 
          state.AppendFragment(keyword, node)
          state.AppendState(childState)
        elif childState.StartsWithClause then 
          state.IsAvailable <- true
          state.AppendState(childState)
        state
      | And(keyword, nodeList)
      | Or(keyword, nodeList) ->
        if state.IsAvailable then 
          state.AppendFragment(keyword)
        List.fold visitNode state nodeList
      | Parens(statement) ->
        match statement with
        | EmptyStatement(s) -> 
          state.IsAvailable <- true
          state.AppendFragment "("
          state.AppendFragment s
          state.AppendFragment ")"
        | _ -> 
          let childState = visitStatement (State(state.Dialect, state.Env, state.ParamIndex)) statement
          if childState.IsAvailable || childState.StartsWithClause then 
            state.IsAvailable <- true
            state.AppendFragment "("
            state.AppendState childState
            state.AppendFragment ")"
        state
      | BindVar(expr, loc) ->
        let result, typ = eval state.Env expr loc
        match result with
        | :? string as s ->
          state.Bind (s, typ)
        | :? IEnumerable as ie -> 
          bindVars state ie
        | param -> 
          state.Bind (param, typ)
        state.IsAvailable <- true ;
        state
      | BindVarComment(expr, _, loc) ->
        state.IsAvailable <- true ;
        state.Bind (eval state.Env expr loc)
        state
      | BindVarsComment(expr, _, loc) ->
        let result, typ = eval state.Env expr loc
        match result with 
        | :? IEnumerable as ie -> 
          bindVars state ie
        | param -> 
          state.Bind (param, typ)
        state.IsAvailable <- true
        state
      | EmbeddedVarComment(expr, loc) -> 
        let value, _ = eval state.Env expr loc
        let fragment = string value
        if not <| String.IsNullOrWhiteSpace fragment then
          state.IsAvailable <- true
          state.AppendFragment fragment
        state
      | IfBlock(ifComment, elifCommentList, elseComment, nodeList) ->
        let eval expr loc  = 
          let result, _ = eval state.Env expr loc
          true.Equals(result)
        let state =
          match () with 
          | If ifComment eval nodeList 
          | Elif elifCommentList eval nodeList 
          | Else elseComment nodeList -> foldInsideBlock state nodeList
          | _ -> state
        List.fold visitNode state nodeList
      | ForBlock(forComment, nodeList) -> 
        let preservedEnv = state.Env
        let state = 
          match forComment with 
          | ForComment (expr, nodeList, loc) ->
            let obj, typ = eval preservedEnv expr loc
            match obj with
            | :? Tuple<string, seq<obj>> as tuple when typ = typeof<Tuple<string, seq<obj>>> ->
              let ident = tuple.Item1
              let seq = tuple.Item2
              let currentEnv = new Dictionary<string, obj * Type>(preservedEnv)
              let hasNextKey = ident + "_has_next"
              currentEnv.Remove(ident) |> ignore
              currentEnv.Remove(hasNextKey) |> ignore
              state.Env <- currentEnv
              seq
              |> Seq.peek
              |> Seq.map (fun (element, hasNext) -> 
                currentEnv.Remove(ident) |> ignore
                currentEnv.Add(ident, (element, typeof<obj>)) 
                currentEnv.Remove(hasNextKey) |> ignore
                currentEnv.Add(hasNextKey, (box hasNext, typeof<bool>)) 
                () )
              |> Seq.fold (fun (state:State) _ -> foldInsideBlock state nodeList) state
            | _ -> 
              let message = appendDetail (SR.TRANQ2014 (expr, typ.FullName)) sql loc
              raise <| SqlException (message)
        state.Env <- preservedEnv
        List.fold visitNode state nodeList
    let state = visitStatement initState statement
    state.Build()

  let internal resolveOrderByEmbeddedVariables sql env statement =
    let level = ref 0
    let insideOrderBy = ref false
    let rec visitStatement (buf:StringBuilder) = 
      function
      | Statement nodeList -> 
        List.fold visitNode buf nodeList
      | Set(set, lhs, rhs, _) ->
        SqlRewriteHelper.writeSet (set, lhs, rhs) buf visitStatement
    and visitNode (buf:StringBuilder) =
      function
      | Word(fragment)
      | Other(fragment)
      | Literal(fragment) 
      | Whitespaces(fragment)
      | Newline(fragment) 
      | BlockComment(fragment)
      | LineComment(fragment) ->
        buf.Append fragment
      | Select(keyword, nodeList, _)
      | From(keyword, nodeList, _) 
      | Where(keyword, nodeList, _)
      | GroupBy(keyword, nodeList, _)
      | Having(keyword, nodeList, _)
      | ForUpdate(keyword, nodeList, _)
      | And(keyword, nodeList)
      | Or(keyword, nodeList) ->
        buf.Append keyword |> ignore
        List.fold visitNode buf nodeList
      | OrderBy(keyword, nodeList, _) -> 
        buf.Append keyword |> ignore
        insideOrderBy := true
        let buf = List.fold visitNode buf nodeList
        insideOrderBy := false
        buf
      | Parens(statement) ->
        SqlRewriteHelper.writeParens statement level buf visitStatement
      | BindVar(expression, _) ->
        buf.Append expression
      | BindVarComment(expression, node, _)
      | BindVarsComment(expression, node, _) ->
        SqlRewriteHelper.writeBindVarComment (expression, node) buf visitNode
      | EmbeddedVarComment(expression, loc) ->
        if !insideOrderBy then
          let evalResult = evaluate env expression loc sql
          buf.Append (fst evalResult)
        else
          SqlRewriteHelper.writeEmbeddedVarComment expression buf
      | IfBlock(ifComment, elifCommentList, elseComment, nodeList) ->
        SqlRewriteHelper.writeIfBlock (ifComment, elifCommentList, elseComment, nodeList) buf visitNode
      | ForBlock(forComment, nodeList) -> 
        SqlRewriteHelper.writeForBlock (forComment, nodeList) buf visitNode
    let buf = visitStatement (StringBuilder(200)) statement
    buf.ToString()

  let internal prepareCore (dialect: IDialect) sql env =
    let statement = dialect.ParseSql sql
    generate (State(dialect, env, 0)) sql statement

  let internal makeEnv(parameters) =
    let dict = Dictionary<string, obj * Type>() :> IDictionary<string, obj * Type>
    parameters 
    |> List.iter(fun (Param(key, value, typ)) -> 
      if dict.ContainsKey(key) then
        dict.Remove(key) |> ignore
      dict.[key] <- (value, typ) )
    dict

  let internal prepare (dialect: IDialect) sql parameters =
    let env = concatEnv dialect.Env (makeEnv parameters)
    prepareCore dialect sql env

  let internal preparePaginate (dialect: IDialect) sql parameters (range: Range)  =
    let statement = dialect.ParseSql sql
    let env = concatEnv dialect.Env (makeEnv parameters)
    let sql = resolveOrderByEmbeddedVariables sql env statement
    let statement = dialect.ParseSql sql
    let sql, env = dialect.RewriteForPagination (statement, sql, env, range.Offset, range.Limit)
    let env = concatEnv dialect.Env env
    prepareCore dialect sql env

  let internal  preparePaginateAndCount (dialect: IDialect) sql parameters (range: Range) =
    let statement = dialect.ParseSql sql
    let env = concatEnv dialect.Env (makeEnv parameters)
    let sql = resolveOrderByEmbeddedVariables sql env statement
    let statement = dialect.ParseSql sql
    let newSql, newEnv = dialect.RewriteForCalcPagination (statement, sql, env, range.Offset, range.Limit)
    let newEnv = concatEnv dialect.Env newEnv
    let paginatePs = prepareCore dialect newSql newEnv
    let newSql, newEnv = dialect.RewriteForCount (statement, sql, env)
    let newEnv = concatEnv dialect.Env newEnv
    let countPs = prepareCore dialect newSql newEnv
    paginatePs, countPs

  let internal prepareFind (dialect: IDialect) (idList:obj list) (entityMeta:EntityMeta) =
    let buf = SqlBuilder(dialect)
    buf.Append("select ")
    entityMeta.PropMetaList 
    |> List.iter (fun propMeta ->
      buf.Append(propMeta.SqlColumnName)
      buf.Append(", ") )
    buf.CutBack(2)
    buf.Append(" from ")
    buf.Append(entityMeta.SqlTableName)
    buf.Append(" where ")
    (idList, entityMeta.IdPropMetaList)
    ||> List.iter2 (fun id propMeta -> 
      buf.Append(propMeta.SqlColumnName)
      buf.Append(" = ")
      buf.Bind(id, propMeta.Type)
      buf.Append(" and "))
    buf.CutBack(5)
    buf.Build()

  let inline private isTargetPropMeta (entity:obj) (opt: ^a) (propMeta:PropMeta) customExclusionRule =
    let exclud = (^a : (member Exclude: seq<string>) opt)
    let includ = (^a : (member Include: seq<string>) opt)
    let excludeNone = (^a : (member ExcludeNone: bool) opt)
    let propName = propMeta.PropName
    if customExclusionRule(propMeta.PropCase) then
      false
    elif (match propMeta.PropCase with Version _ -> true | _ -> false) then
      true
    elif excludeNone && propMeta.GetValue(entity) = null then
      false
    elif not <| Seq.isEmpty includ then
      Seq.exists ((=) propName) includ &&  not <| Seq.exists ((=) propName) exclud
    else
      not <| Seq.exists ((=) propName) exclud

  let internal prepareInsert (dialect: IDialect) (entity:obj) (entityMeta:EntityMeta) (opt:InsertOpt) =
    let propMetaSeq =
      entityMeta.PropMetaList
      |> Seq.filter (fun propMeta -> 
        propMeta.IsInsertable )
      |> Seq.filter (fun propMeta ->
        isTargetPropMeta entity opt propMeta (function Id Identity | Version VersionKind.Computed -> true | _ -> false) )
    if Seq.isEmpty propMetaSeq then
      raise <| SqlException(SR.TRANQ4024())
    let buf = SqlBuilder(dialect)
    buf.Append("insert into ")
    buf.Append(entityMeta.SqlTableName)
    buf.Append(" ( ")
    propMetaSeq 
    |> Seq.iter (fun propMeta ->
      buf.Append(propMeta.SqlColumnName)
      buf.Append(", ") )
    buf.CutBack(2)
    buf.Append(" ) values ( ")
    propMetaSeq 
    |> Seq.iter (fun propMeta ->
      buf.Bind(propMeta.GetValue entity, propMeta.Type)
      buf.Append(", ") )
    buf.CutBack(2)
    buf.Append(" )")
    buf.Build()

  let internal prepareUpdate (dialect: IDialect) (entity:obj) (entityMeta:EntityMeta) (opt:UpdateOpt) =
    let incremetedVersion =
      entityMeta.VersionPropMeta
      |> Option.bind (fun propMeta -> 
        match propMeta.PropCase with
        | Version VersionKind.Incremented -> Some propMeta 
        | _ -> None)
    let propMetaSeq =
      entityMeta.PropMetaList
      |> Seq.filter (fun propMeta -> 
        propMeta.IsUpdatable )
      |> Seq.filter (fun propMeta -> 
        isTargetPropMeta entity opt propMeta (function Id _ | Version _ -> true | _ -> false) )
    if Seq.isEmpty propMetaSeq && (opt.IgnoreVersion || incremetedVersion.IsNone) then
      raise <| SqlException(SR.TRANQ4025())
    let buf = SqlBuilder(dialect)
    buf.Append("update ")
    buf.Append(entityMeta.SqlTableName)
    buf.Append(" set ")
    propMetaSeq
    |> Seq.iter (fun propMeta ->
      buf.Append(propMeta.SqlColumnName)
      buf.Append(" = ")
      buf.Bind(propMeta.GetValue(entity), propMeta.Type)
      buf.Append(", ") )
    buf.CutBack(2)
    if not opt.IgnoreVersion then
      incremetedVersion
      |> Option.iter (fun propMeta ->
        buf.Append ", "
        buf.Append (propMeta.SqlColumnName)
        buf.Append " = "
        buf.Append (propMeta.SqlColumnName)
        buf.Append " + 1" )
    buf.Append(" where ")
    entityMeta.IdPropMetaList 
    |> Seq.iter (fun propMeta -> 
      buf.Append(propMeta.SqlColumnName)
      buf.Append(" = ")
      buf.Bind(propMeta.GetValue(entity), propMeta.Type)
      buf.Append(" and ") )
    buf.CutBack(5)
    if not opt.IgnoreVersion then
      entityMeta.VersionPropMeta
      |> Option.iter (fun propMeta -> 
        buf.Append " and "
        buf.Append(propMeta.SqlColumnName)
        buf.Append(" = ")
        buf.Bind(propMeta.GetValue(entity), propMeta.Type) )
    buf.Build ()

  let internal prepareDelete (dialect: IDialect) (entity:obj) (entityMeta:EntityMeta) (opt:DeleteOpt) =
    let buf = SqlBuilder(dialect)
    buf.Append("delete from ")
    buf.Append(entityMeta.SqlTableName)
    buf.Append(" where ")
    entityMeta.IdPropMetaList
    |> Seq.iter (fun propMeta -> 
      buf.Append(propMeta.SqlColumnName)
      buf.Append(" = ")
      buf.Bind(propMeta.GetValue(entity), propMeta.Type)
      buf.Append(" and "))
    buf.CutBack(5)
    if not opt.IgnoreVersion then
      entityMeta.VersionPropMeta
      |> Option.iter (fun propMeta -> 
        buf.Append " and "
        buf.Append(propMeta.SqlColumnName)
        buf.Append(" = ")
        buf.Bind(propMeta.GetValue(entity), propMeta.Type) )
    buf.Build ()

  let internal prepareCall (dialect: IDialect) procedure (procedureMeta:ProcedureMeta) =
    let getDirection = function
      | Input -> Direction.Input
      | InputOutput -> Direction.InputOutput
      | Output -> Direction.Output
      | ReturnValue -> Direction.ReturnValue
      | Result _ -> Direction.Result
      | Unit -> failwith "unreachable."
    let parameters = 
      procedureMeta.ProcedureParamMetaList
      |> Seq.filter (fun paramMeta -> 
          match paramMeta.ParamMetaCase with
          | Unit -> false
          | Result _ -> dialect.IsResultParamRecognizedAsOutputParam
          | _ -> true )
      |> Seq.map (fun paramMeta -> 
        let value, typ, dbType = 
          dialect.ConvertFromClrToDb(paramMeta.GetValue procedure, paramMeta.Type, paramMeta.UdtTypeName)
        let value =
          match paramMeta.ParamMetaCase with
          | Input | InputOutput -> value
          | _ -> box DBNull.Value
        let name = dialect.CreateParamName paramMeta.ParamName
        { Name = name
          Value = value
          Type = typ
          DbType = dbType
          Direction = getDirection paramMeta.ParamMetaCase
          Size = paramMeta.Size
          Precision = paramMeta.Precision
          Scale = paramMeta.Scale
          UdtTypeName = paramMeta.UdtTypeName } )
      |> Seq.toList
    let procedureName = procedureMeta.SqlProcedureName
    { Text = procedureName
      FormattedText = dialect.BuildProcedureCallSql(procedureName, parameters)
      Params = parameters }
