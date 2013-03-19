namespace Tranq

open System
open System.Collections
open System.Collections.Concurrent
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
open Tranq.SqlAst
open Tranq.Text
open Tranq.SqlParser

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal BuiltinFun =

  let isSome (obj:obj) = obj <> null

  let isNone (obj:obj) = obj = null

  let isNoneOrEmpty (obj:obj) = 
    match obj with
    | :? string as x -> 
      String.IsNullOrEmpty(x)
    | :? option<string> as x-> 
      match x with None -> true | Some x -> String.IsNullOrEmpty(x)
    | _ -> 
      if obj = null then true else String.IsNullOrEmpty(string obj)

  let isNoneOrWhiteSpace (obj:obj) = 
    match obj with
    | :? string as x -> 
      String.IsNullOrWhiteSpace(x)
    | :? option<string> as x-> 
      match x with None -> true | Some x -> String.IsNullOrWhiteSpace(x)
    | _ -> 
      if obj = null then true else String.IsNullOrWhiteSpace(string obj)

  let date (obj:obj) = 
    match obj with
    | :? DateTime as x -> 
      Some x.Date
    | :? option<DateTime> as x-> 
      match x with
      | Some dateTime ->
        Some dateTime.Date
      | __ -> None
    | _ -> 
      if obj <> null then 
        let d = Convert.ToDateTime(obj)
        Some d.Date
      else
        None

  let nextDate (obj:obj) =
    date obj 
    |> Option.map(fun v -> v.Date.Add(TimeSpan(1, 0, 0, 0)))

  let prevDate (obj:obj) =
    date obj 
    |> Option.map(fun v -> v.Date.Subtract(TimeSpan(1, 0, 0, 0)))

  let escape (dilaect: IDialect) (obj:obj) = 
    match obj with
    | :? string as x -> 
      dilaect.EscapeMetaChars x
    | :? option<string> as x-> 
      match x with None -> null | Some x -> dilaect.EscapeMetaChars x
    | _ -> 
      if obj = null then null else dilaect.EscapeMetaChars(string obj)

  let prefix dilaect (obj:obj) = 
    let s = escape dilaect obj
    if s = null then null else s + "%"

  let infix dilaect (obj:obj) = 
    let s = escape dilaect obj
    if s = null then null else "%" + s + "%"

  let suffix dilaect (obj:obj) = 
    let s = escape dilaect obj
    if s = null then null else "%" + s

  let createRootEnv dialect =
    let escape = escape dialect
    let prefix = prefix dialect
    let infix = infix dialect
    let suffix = suffix dialect
    dict [
      "isSome", (box isSome, isSome.GetType())
      "isNone", (box isNone, isNone.GetType())
      "isNoneOrEmpty", (box isNoneOrEmpty, isNoneOrEmpty.GetType())
      "isNoneOrWhiteSpace", (box isNoneOrWhiteSpace, isNoneOrWhiteSpace.GetType())
      "date", (box date, date.GetType())
      "nextDate", (box nextDate, nextDate.GetType())
      "prevDate", (box prevDate, prevDate.GetType())
      "escape", (box escape, escape.GetType())
      "prefix", (box prefix, prefix.GetType())
      "infix", (box infix, infix.GetType())
      "suffix", (box suffix, suffix.GetType()) ]

/// An abstract base class for IDialect implementations
[<AbstractClass>]
type DialectBase(dataConvReg) as this = 

  let lazyRootEnv = lazy (BuiltinFun.createRootEnv this)

  let emptyDisposer = { new IDisposable with member this.Dispose() = () }

  let statementCache = ConcurrentDictionary<string, Lazy<SqlAst.Statement>>()

  abstract Name : string

  abstract DataConvRegistry : DataConvRegistry
  default this.DataConvRegistry = dataConvReg

  abstract CanGetIdentityAtOnce : bool
  default this.CanGetIdentityAtOnce = false

  abstract CanGetIdentityAndVersionAtOnce : bool
  default this.CanGetIdentityAndVersionAtOnce = false

  abstract CanGetVersionAtOnce : bool
  default this.CanGetVersionAtOnce = false

  abstract IsResultParamRecognizedAsOutputParam : bool
  default this.IsResultParamRecognizedAsOutputParam = false

  abstract IsHasRowsPropertySupported : bool
  default this.IsHasRowsPropertySupported = true

  abstract Env : IDictionary<string, obj * Type>
  default this.Env = Lazy.force lazyRootEnv

  abstract CountFunction : string
  default this.CountFunction = "count"

  abstract EscapeMetaChars : string -> string

  abstract PrepareIdentitySelect : string * string -> PreparedStatement
  default this.PrepareIdentitySelect(tableName, idColumnName) = raise <| NotSupportedException("GetIdentitySelectSql")

  abstract PrepareIdentityAndVersionSelect : string * string * string -> PreparedStatement
  default this.PrepareIdentityAndVersionSelect(tableName, idColumnName, versionColumnName) = raise <| NotSupportedException("GetIdentityAndVersionSelectSql")

  abstract PrepareVersionSelect : string * string * list<string * obj * Type> -> PreparedStatement
  default this.PrepareVersionSelect(tableName, versionColumnName, idMetaList) = raise <| NotSupportedException("GetVersionSelectSql")

  abstract PrepareSequenceSelect : string -> PreparedStatement
  default this.PrepareSequenceSelect(sequenceName) = raise <| NotSupportedException("PrepareSequenceSelect")

  abstract ConvertFromDbToUnderlyingClr : obj * Type -> obj
  default this.ConvertFromDbToUnderlyingClr (dbValue:obj, destType:Type) = 
    if dbValue.GetType() = destType then
      dbValue
    else
      Convert.ChangeType(dbValue, destType)

  abstract ConvertFromDbToClr : obj * Type * string * PropertyInfo-> obj
  default this.ConvertFromDbToClr (dbValue:obj, destType, udtTypeName, destProp) = 
    let toEnumObject (enumType:Type) underlyingValue =
      let underlyingType = enumType.GetEnumUnderlyingType()
      let underlyingValue = this.ConvertFromDbToUnderlyingClr(underlyingValue, underlyingType)
      Enum.ToObject(enumType, underlyingValue)
    let toRichObject basicType compose dbValue =
      if Type.isOption basicType then
        let dbValue = if dbValue = null || Convert.IsDBNull(dbValue) then null else dbValue
        compose (Option.make basicType dbValue)
      else
        if dbValue = null || Convert.IsDBNull(dbValue) then 
          null
        else
          compose dbValue
    match dataConvReg.TryGet(destType) with
    | Some(basicType, compose, _) ->
      let dbValue = if dbValue = null || Convert.IsDBNull(dbValue) then null else dbValue
      toRichObject basicType compose dbValue
    | _ -> 
      if dbValue = null || Convert.IsDBNull(dbValue) then
        null
      else
        match dataConvReg.TryGet(destType) with
        | Some(basicType, compose, _) -> 
          toRichObject basicType compose dbValue
        | _ -> 
          if dbValue.GetType() = destType then
            dbValue
          elif Type.isOption destType then
            let elementType = Option.getElementType destType
            let value = 
              if elementType.IsEnum then 
                toEnumObject elementType dbValue
              else 
                match dataConvReg.TryGet(elementType) with
                | Some(basicType, compose, _) -> 
                  toRichObject basicType compose dbValue
                | _ -> 
                  this.ConvertFromDbToUnderlyingClr(dbValue, elementType)
            Option.make destType value
          elif destType.IsEnum then
            toEnumObject destType dbValue
          else
            this.ConvertFromDbToUnderlyingClr(dbValue, destType)

  // http://msdn.microsoft.com/ja-jp/library/yy6y35y8.aspx
  // http://msdn.microsoft.com/ja-jp/library/cc716729.aspx
  abstract MapClrTypeToDbType : Type -> DbType
  default this.MapClrTypeToDbType (clrType:Type) =
    match clrType with
    | t when t = typeof<String> -> DbType.String
    | t when t = typeof<Int32> || t = typeof<Int32 Nullable> -> DbType.Int32
    | t when t = typeof<Int64> || t = typeof<Int64 Nullable> -> DbType.Int64
    | t when t = typeof<Decimal> || t = typeof<Decimal Nullable> -> DbType.Decimal
    | t when t = typeof<DateTime> || t = typeof<DateTime Nullable> -> DbType.DateTime
    | t when t = typeof<TimeSpan> || t = typeof<TimeSpan Nullable> -> DbType.Time
    | t when t = typeof<Boolean> || t = typeof<Boolean Nullable> -> DbType.Boolean
    | t when t = typeof<Byte> || t = typeof<Byte Nullable> -> DbType.Byte
    | t when t = typeof<Byte[]> -> DbType.Binary
    | t when t = typeof<DateTimeOffset> || t = typeof<DateTimeOffset Nullable> -> DbType.DateTimeOffset
    | t when t = typeof<Double> || t = typeof<Double Nullable> -> DbType.Double
    | t when t = typeof<Single> || t = typeof<Single Nullable> -> DbType.Single
    | t when t = typeof<Guid> || t = typeof<Guid Nullable> -> DbType.Guid
    | t when t = typeof<Int16> || t = typeof<Int16 Nullable> -> DbType.Int16
    | t when t = typeof<UInt16> || t = typeof<UInt16 Nullable> -> DbType.UInt16
    | t when t = typeof<UInt32> || t = typeof<UInt32 Nullable> -> DbType.UInt32
    | t when t = typeof<UInt64> || t = typeof<UInt64 Nullable> -> DbType.UInt64
    | t when t = typeof<Object> -> DbType.Object
    | _ -> DbType.String
  
  abstract ConvertFromClrToDb : obj * Type * string -> obj * Type * DbType
  default this.ConvertFromClrToDb (clrValue, srcType, udtTypeName) = 
    let convert clrValue srcType =
      let value, typ =
        if Type.isOption srcType then
          Option.getElement srcType clrValue
        else
          clrValue, srcType
      if value = null || Convert.IsDBNull(value) then 
        if typ.IsEnum then
          Convert.DBNull, typ.GetEnumUnderlyingType()
        else 
          Convert.DBNull, typ
      else
        if typ.IsEnum then
          let text = Enum.Format(typ, value, "D")
          match typ.GetEnumUnderlyingType() with
          | t when t = typeof<Int32> -> upcast int32 text, t
          | t when t = typeof<Int64> -> upcast int64 text, t
          | t when t = typeof<Byte> -> upcast byte text, t
          | t when t = typeof<SByte> -> upcast sbyte text, t
          | t when t = typeof<Int16> -> upcast int16 text, t
          | t when t = typeof<UInt16> -> upcast uint16 text, t
          | t when t = typeof<UInt32> -> upcast uint32 text, t
          | t when t = typeof<UInt64> -> upcast uint64 text, t
          | _ -> failwith "unreachable."
        else 
          value, typ
    let value, typ = convert clrValue srcType
    let value, typ = 
      if FSharpType.IsUnion typ then
        value, (FSharpType.GetUnionCases typ).[0].DeclaringType
      else 
        value, typ
    let value, typ = 
      match dataConvReg.TryGet(typ) with
      | Some(basicType, _, decompose) -> 
        if value = Convert.DBNull then 
          convert null basicType
        else
          let basic = decompose value
          convert basic basicType
      | _ -> value, typ
    value, typ, this.MapClrTypeToDbType typ
  
  abstract FormatAsSqlLiteral : obj * Type * DbType -> string
  default this.FormatAsSqlLiteral (dbValue:obj, clrType:Type, dbType:DbType) =
    if Convert.IsDBNull dbValue then 
      "null"
    else 
      let quote () = "'" + string dbValue + "'"
      match dbType with
      | d when d = DbType.String || d = DbType.StringFixedLength->
        "N'" + string dbValue + "'"
      | d when d = DbType.Time ->
        match dbValue with
        | :? TimeSpan as t -> 
          "'" + t.ToString("c") + "'"
        | _ -> 
          quote ()
      | d when d = DbType.Date ->
        match dbValue with
        | :? DateTime as d -> 
          "'" + d.ToString("yyyy-MM-dd") + "'"
        | _ -> 
          quote ()
      | d when d = DbType.DateTime ->
        match dbValue with
        | :? DateTime as d -> 
          "'" + d.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'"
        | _ -> 
          quote ()
      | d when d = DbType.DateTime2 ->
        match dbValue with
        | :? DateTime as d -> 
          "'" + d.ToString("yyyy-MM-dd HH:mm:ss.fffffff") + "'"
        | _ -> 
          quote ()
      | d when d = DbType.DateTimeOffset ->
        match dbValue with
        | :? DateTimeOffset as d -> 
          "'" + d.ToString("yyyy-MM-dd HH:mm:ss K") + "'"
        | _ -> 
          quote ()
      | d when d = DbType.Binary ->
        "/** binary value is not shown */null"
      | d when d = DbType.Boolean ->
        match dbValue with
        | :? bool as b -> 
          if b then "'1'" else "'0'"
        | _ -> 
          quote ()
      | d when d = DbType.Guid ->
          quote ()
      | _ -> 
        string dbValue
  
  abstract CreateParamName : int -> string
  default this.CreateParamName (index:int) =
    "@p" + string index
  
  abstract CreateParamName : string -> string
  default this.CreateParamName (baseName:string) =
    if baseName.StartsWith "@" then
      baseName
    else
      "@" + baseName
 
  abstract IsUniqueConstraintViolation : exn -> bool
  
  abstract RewriteForPagination : SqlAst.Statement * string * IDictionary<string, obj * Type> * int64 * int64 -> string * IDictionary<string, obj * Type>
  default this.RewriteForPagination (statement, sql, exprCtxt, offset, limit) = 
    this.RewriteForPaginationWithRowNumber(statement, sql, exprCtxt, offset, limit)
  
  abstract RewriteForCalcPagination : SqlAst.Statement * string * IDictionary<string, obj * Type> * int64 * int64 -> string * IDictionary<string, obj * Type>
  default this.RewriteForCalcPagination (statement, sql, exprCtxt, offset, limit) = 
    this.RewriteForPaginationWithRowNumber(statement, sql, exprCtxt, offset, limit)
  
  member this.RewriteForPaginationWithRowNumber (statement, sql, exprCtxt, offset, limit) = 
    let offset = if offset < 0L then 0L else offset
    let level = ref 0
    let orderByBuf = StringBuilder(100)
    let forUpdateBuf = StringBuilder(100)
    let rec visitStatement (buf:StringBuilder) = 
      function
      | Statement nodeList -> 
        List.fold visitNode buf nodeList
      | Set(set, lhs, rhs, _) ->
        SqlRewriteHelper.writeSet (set, lhs, rhs) buf visitStatement
    and visitNode (buf:StringBuilder) =
      function
      | Word(fragment) ->
        let fragment =
          if buf = orderByBuf then
            let pos = fragment.LastIndexOf '.'
            if pos > 0 then
              "temp_" + fragment.Substring(pos, fragment.Length - pos)
            else
              fragment
          else
            fragment
        buf.Append fragment
      | Other(fragment)
      | Literal(fragment) 
      | Whitespaces(fragment)
      | Newline(fragment) 
      | BlockComment(fragment)
      | LineComment(fragment) ->
        buf.Append fragment
      | OrderBy(keyword, nodeList, _) ->
        let orderByBuf = if !level = 0 then orderByBuf else buf
        orderByBuf.Append keyword |> ignore
        List.fold visitNode orderByBuf nodeList |> ignore
        buf 
      | ForUpdate(keyword, nodeList, _) ->
        let forUpdateBuf = if !level = 0 then forUpdateBuf else buf
        forUpdateBuf.Append keyword |> ignore
        List.fold visitNode forUpdateBuf nodeList |> ignore
        buf 
      | Select(keyword, nodeList, _)
      | From(keyword, nodeList, _) 
      | Where(keyword, nodeList, _)
      | GroupBy(keyword, nodeList, _)
      | Having(keyword, nodeList, _)
      | And(keyword, nodeList)
      | Or(keyword, nodeList) ->
        buf.Append keyword |> ignore
        List.fold visitNode buf nodeList
      | Parens(statement) ->
        SqlRewriteHelper.writeParens statement level buf visitStatement
      | BindVar(expression, _) ->
        buf.Append expression
      | BindVarComment(expression, node, _)
      | BindVarsComment(expression, node, _) ->
        SqlRewriteHelper.writeBindVarComment (expression, node) buf visitNode
      | EmbeddedVarComment(expression, _)->
        SqlRewriteHelper.writeEmbeddedVarComment expression buf
      | IfBlock(ifComment, elifCommentList, elseComment, nodeList) ->
        SqlRewriteHelper.writeIfBlock (ifComment, elifCommentList, elseComment, nodeList) buf visitNode
      | ForBlock(forComment, nodeList) -> 
        SqlRewriteHelper.writeForBlock (forComment, nodeList) buf visitNode
    let subqueryBuf = visitStatement (StringBuilder(200)) statement
    if orderByBuf.Length = 0 then
      let message = Sql.appendSimpleDetail (SR.TRANQ2016 ()) sql
      raise <| SqlException (message)
    let buf = StringBuilder(400)
    buf.Append "select * from ( select temp_.*, row_number() over( " |> ignore
    buf.Append orderByBuf |> ignore
    buf.Append " ) as tranq_rownumber_ from ( " |> ignore
    buf.Append subqueryBuf |> ignore
    buf.Append ") temp_ ) temp2_ where " |> ignore
    buf.Append "tranq_rownumber_ > " |> ignore
    buf.Append "/* tranq_offset */" |> ignore
    buf.Append offset |> ignore
    if limit >= 0L then
      buf.Append " and " |> ignore
      buf.Append "tranq_rownumber_ <= " |> ignore
      buf.Append "/* tranq_offset + tranq_limit */" |> ignore
      buf.Append (offset + limit) |> ignore
    if forUpdateBuf.Length > 0 then
      buf.Append " " |> ignore
      buf.Append forUpdateBuf |> ignore
    let exprCtxt = Dictionary<string, obj * Type>(exprCtxt) :> IDictionary<string, obj * Type>
    exprCtxt.["tranq_offset"] <- (box offset, typeof<int>)
    exprCtxt.["tranq_limit"] <- (box limit, typeof<int>)
    buf.ToString(), exprCtxt
  
  abstract RewriteForCount : SqlAst.Statement * string * IDictionary<string, obj * Type> -> string * IDictionary<string, obj * Type>
  default this.RewriteForCount (statement, sql, exprCtxt) = 
    let level = ref 0
    let rec visitStatement (buf:StringBuilder) = 
      function
      | Statement nodeList -> 
        List.fold visitNode buf nodeList
      | Set(set, lhs, rhs, _) ->
        SqlRewriteHelper.writeSet (set, lhs, rhs) buf visitStatement
    and visitNode buf =
      function
      | Word(fragment)
      | Other(fragment)
      | Literal(fragment) 
      | Whitespaces(fragment)
      | Newline(fragment) 
      | BlockComment(fragment)
      | LineComment(fragment) ->
        buf.Append fragment
      | OrderBy(keyword, nodeList, _) ->
        if !level <> 0 then
          buf.Append keyword |> ignore
          List.fold visitNode buf nodeList
        else
          buf
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
      | Parens(statement) ->
        SqlRewriteHelper.writeParens statement level buf visitStatement
      | BindVar(expression, _) ->
        buf.Append expression
      | BindVarComment(expression, node, _)
      | BindVarsComment(expression, node, _) ->
        SqlRewriteHelper.writeBindVarComment (expression, node) buf visitNode
      | EmbeddedVarComment(expression, _)->
        SqlRewriteHelper.writeEmbeddedVarComment expression buf
      | IfBlock(ifComment, elifCommentList, elseComment, nodeList) ->
        SqlRewriteHelper.writeIfBlock (ifComment, elifCommentList, elseComment, nodeList) buf visitNode
      | ForBlock(forComment, nodeList) -> 
        SqlRewriteHelper.writeForBlock (forComment, nodeList) buf visitNode
    let buf = visitStatement (StringBuilder(200)) statement
    buf.Insert(0, "select " + this.CountFunction + "(*) from ( ") |> ignore
    buf.Append " ) t_" |> ignore
    buf.ToString(), exprCtxt
  
  abstract BuildProcedureCallSql : string * PreparedParam seq -> string
 
  abstract EncloseIdentifier : string -> string
 
  abstract SetupDbParam : PreparedParam * DbParameter -> unit
  default this.SetupDbParam(param, dbParam) =
    let dbParam = dbParam :> IDbDataParameter
    dbParam.ParameterName <- param.Name
    dbParam.DbType <- param.DbType
    dbParam.Direction <-
      match param.Direction with
      | Direction.Input -> ParameterDirection.Input
      | Direction.InputOutput -> ParameterDirection.InputOutput
      | Direction.Output -> ParameterDirection.Output
      | Direction.ReturnValue -> ParameterDirection.ReturnValue
      | Direction.Result -> ParameterDirection.Output
      | _ -> failwith "unreachable."
    Option.iter (fun size -> dbParam.Size <- size) param.Size
    Option.iter (fun precision -> dbParam.Precision <- precision) param.Precision
    Option.iter (fun scale -> dbParam.Scale <- scale) param.Scale
    dbParam.Value <- param.Value

  abstract GetValue : DbDataReader * int * PropertyInfo -> obj
  default this.GetValue(reader, index, destProp) =
    reader.GetValue(index)

  abstract MakeParamDisposer : DbCommand -> IDisposable
  default this.MakeParamDisposer(command) = 
    emptyDisposer

  abstract ParseSql : string -> SqlAst.Statement
  default this.ParseSql(text) = 
    statementCache.GetOrAdd(text, Lazy(fun () -> Sql.parse text))
    |> Lazy.force
    
  interface IDialect with
    member this.Name = this.Name
    member this.DataConvRegistry = this.DataConvRegistry
    member this.CanGetIdentityAtOnce = this.CanGetIdentityAtOnce
    member this.CanGetIdentityAndVersionAtOnce = this.CanGetIdentityAndVersionAtOnce
    member this.CanGetVersionAtOnce = this.CanGetVersionAtOnce
    member this.IsResultParamRecognizedAsOutputParam = this.IsResultParamRecognizedAsOutputParam
    member this.IsHasRowsPropertySupported = this.IsHasRowsPropertySupported
    member this.Env = this.Env
    member this.EscapeMetaChars(text) = this.EscapeMetaChars(text)
    member this.PrepareIdentitySelect(tableName, idColumnName) = this.PrepareIdentitySelect(tableName, idColumnName)
    member this.PrepareIdentityAndVersionSelect(tableName, idColumnName, versionColumnName) = this.PrepareIdentityAndVersionSelect(tableName, idColumnName, versionColumnName)
    member this.PrepareVersionSelect(tableName, versionColumnName, idMetaList) = this.PrepareVersionSelect(tableName, versionColumnName, idMetaList)
    member this.PrepareSequenceSelect(sequenceName) = this.PrepareSequenceSelect(sequenceName)
    member this.ConvertFromDbToClr(value, typ, string, destProp) = this.ConvertFromDbToClr(value, typ, string, destProp)
    member this.ConvertFromClrToDb(value, typ, string) = this.ConvertFromClrToDb(value, typ, string)
    member this.FormatAsSqlLiteral(value, clrType, dbType) = this.FormatAsSqlLiteral(value, clrType, dbType)
    member this.CreateParamName(index:int) = this.CreateParamName(index)
    member this.CreateParamName(baseName:string) = this.CreateParamName(baseName)
    member this.IsUniqueConstraintViolation(exn) = this.IsUniqueConstraintViolation (exn)
    member this.RewriteForPagination(statement, sql, exprCtxt, offset, limit) = this.RewriteForPagination(statement, sql, exprCtxt, offset, limit)
    member this.RewriteForCalcPagination(statement, sql, exprCtxt, offset, limit) = this.RewriteForCalcPagination(statement, sql, exprCtxt, offset, limit)
    member this.RewriteForCount(statement, sql, exprCtxt) =  this.RewriteForCount(statement, sql, exprCtxt)
    member this.BuildProcedureCallSql(procedureName, parameters) = this.BuildProcedureCallSql(procedureName, parameters)
    member this.EncloseIdentifier(identifier) = this.EncloseIdentifier(identifier)
    member this.SetupDbParam(param, dbParam) = this.SetupDbParam(param, dbParam)
    member this.GetValue(reader, index, destProp) = this.GetValue(reader, index, destProp)
    member this.MakeParamDisposer(command) = this.MakeParamDisposer(command)
    member this.ParseSql(text) = this.ParseSql(text)

/// The dialect for Microsoft SQLServer 2008 and higher
type MsSqlDialect(?dataConvReg) = 
  inherit DialectBase(defaultArg dataConvReg (DataConvRegistry()))

  let escapeRegex = Regex(@"[$_%\[]")

  override this.Name = "MsSql"

  override this.CanGetIdentityAtOnce = true

  override this.CanGetIdentityAndVersionAtOnce = true

  override this.CanGetVersionAtOnce = true

  override this.CountFunction = "count_big"

  override this.EscapeMetaChars(text) =
    escapeRegex.Replace(text, "$$$&")

  override this.PrepareIdentitySelect(tableName, idColumnName) = 
    let buf = SqlBuilder(this)
    buf.Append "select scope_identity() where @@rowcount = 1"
    buf.Build()

  override this.PrepareIdentityAndVersionSelect(tableName, idColumnName, versionColumnName) = 
    let buf = SqlBuilder(this)
    buf.Append "select scope_identity(), " 
    buf.Append versionColumnName 
    buf.Append " from "
    buf.Append tableName
    buf.Append " where "
    buf.Append idColumnName
    buf.Append " = scope_identity() and @@rowcount = 1"
    buf.Build()

  override this.PrepareVersionSelect(tableName, versionColumnName, idMetaList) = 
    let buf = SqlBuilder(dialect = this, paramNameSuffix = "x")
    buf.Append "select "
    buf.Append versionColumnName
    buf.Append " from "
    buf.Append tableName
    buf.Append " where "
    idMetaList
    |> List.iter (fun (idColumnName, idValue, idType) -> 
      buf.Append idColumnName
      buf.Append " = "
      buf.Bind(idValue, idType)
      buf.Append " and ")
    buf.Append "@@rowcount = 1"
    buf.Build()
 
  override this.IsUniqueConstraintViolation(exn:exn) = 
    match exn with
    | :? System.Data.SqlClient.SqlException as ex -> 
      match ex.Number with
      | 2601 | 2627 -> true
      | _ -> false
    | _ -> 
      false
 
  override this.RewriteForPagination (statement, sql, exprCtxt, offset, limit) =
    if (offset <= 0L && limit > 0L) || limit = 0L then
      this.RewriteForPaginationWithTop (statement, sql, exprCtxt, limit)
    else
      base.RewriteForPagination (statement, sql, exprCtxt, offset, limit)
 
  override this.RewriteForCalcPagination (statement, sql, exprCtxt, offset, limit) =
    if (offset <= 0L && limit > 0L) || limit = 0L then
      this.RewriteForPaginationWithTop (statement, sql, exprCtxt, limit)
    else
      base.RewriteForCalcPagination (statement, sql, exprCtxt, offset, limit)

  member this.RewriteForPaginationWithTop (statement, sql, exprCtxt, limit) =
    let limit = if limit < 0L then 0L else limit
    let level = ref 0
    let rec visitStatement (buf:StringBuilder) = 
      function
      | Statement nodeList -> 
        List.fold visitNode buf nodeList
      | Set(set, lhs, rhs, _) ->
        SqlRewriteHelper.writeSet (set, lhs, rhs) buf visitStatement
    and visitNode buf =
      function
      | Word(fragment)
      | Other(fragment)
      | Literal(fragment) 
      | Whitespaces(fragment)
      | Newline(fragment) 
      | BlockComment(fragment)
      | LineComment(fragment) ->
        buf.Append(fragment)
      | Select(keyword, nodeList, loc) ->
        buf.Append keyword |> ignore
        if !level = 0 then
          buf.Append " top " |> ignore
          buf.Append "(/* tranq_limit */" |> ignore
          buf.Append limit |> ignore
          buf.Append ")" |> ignore
        List.fold visitNode buf nodeList
      | From(keyword, nodeList, _) 
      | Where(keyword, nodeList, _)
      | GroupBy(keyword, nodeList, _)
      | Having(keyword, nodeList, _)
      | OrderBy(keyword, nodeList, _)
      | ForUpdate(keyword, nodeList, _) 
      | And(keyword, nodeList)
      | Or(keyword, nodeList) ->
        let buf = buf.Append keyword
        List.fold visitNode buf nodeList
      | Parens(statement) ->
        SqlRewriteHelper.writeParens statement level buf visitStatement
      | BindVar(expression, _) ->
        buf.Append expression
      | BindVarComment(expression, node, _)
      | BindVarsComment(expression, node, _) ->
        SqlRewriteHelper.writeBindVarComment (expression, node) buf visitNode
      | EmbeddedVarComment(expression, _)->
        SqlRewriteHelper.writeEmbeddedVarComment expression buf
      | IfBlock(ifComment, elifCommentList, elseComment, nodeList) ->
        SqlRewriteHelper.writeIfBlock (ifComment, elifCommentList, elseComment, nodeList) buf visitNode
      | ForBlock(forComment, nodeList) -> 
        SqlRewriteHelper.writeForBlock (forComment, nodeList) buf visitNode
    let buf = visitStatement (StringBuilder(200) )statement
    let exprCtxt = Dictionary<string, obj * Type>(exprCtxt) :> IDictionary<string, obj * Type>
    exprCtxt.["tranq_limit"] <- (box limit, typeof<int>)
    buf.ToString(), exprCtxt
 
  override this.BuildProcedureCallSql(procedureName, parameters) = 
    let getDataType (p:PreparedParam) =
      match p.DbType with
      | d when d = DbType.Int32 -> "int"
      | d when d = DbType.Int64 -> "bigint"
      | d when d = DbType.Boolean -> "bit"
      | d when d = DbType.Date -> "date"
      | d when d = DbType.DateTime -> "datetime"
      | d when d = DbType.DateTime2 -> "datetime2"
      | d when d = DbType.DateTimeOffset -> "datetimeoffset"
      | d when d = DbType.Decimal -> "decimal"
      | d when d = DbType.Binary -> "varbinary"
      | d when d = DbType.Double -> "float"
      | d when d = DbType.String -> "nvarchar"
      | d when d = DbType.StringFixedLength -> "nchar"
      | d when d = DbType.Int16 -> "smallint"
      | d when d = DbType.Time -> "time"
      | d when d = DbType.Byte -> "tinyint"
      | d when d = DbType.Guid -> "uniqueidentifier"
      | _ -> "sql_variant"
    let getInitValue (p:PreparedParam) =
      match p.Direction with
      | Direction.Input
      | Direction.InputOutput -> 
        this.FormatAsSqlLiteral(p.Value, p.Type, p.DbType)
      | _ -> 
        "null"
    let buf = StringBuilder 200
    parameters
    |> Seq.iter (fun p -> 
      buf 
      +> "declare" +> " " +> p.Name +> " " +> (getDataType p) +> "\n" 
      +> "set" +> " " +> p.Name +> " = " +> (getInitValue p) +! "\n" )
    buf 
    +> "exec " 
    +> (parameters
        |> Seq.tryFind (fun p -> p.Direction = Direction.ReturnValue)
        |> function Some p -> p.Name + " = " | _ -> "")
    +> procedureName +! " "
    parameters
    |> Seq.filter (fun p ->
      match p.Direction with
      | Direction.Input | Direction.InputOutput | Direction.Output -> true 
      | _ -> false)
    |> Seq.peek
    |> Seq.iter (fun (p, hasNext) ->
      match p.Direction with
      | Direction.Input ->
        buf +! p.Name
      | Direction.InputOutput
      | Direction.Output -> 
        buf +> p.Name +! " output"
      | _ -> ()
      if hasNext then buf +! ", ")
    buf.ToString()
 
  override this.EncloseIdentifier(identifier) = "[" + identifier + "]"

  // http://msdn.microsoft.com/ja-jp/library/yy6y35y8.aspx
  override this.SetupDbParam(param, dbParam) =
    base.SetupDbParam(param, dbParam)
    match dbParam with
    | :? System.Data.SqlClient.SqlParameter as dbParam -> 
      match param.DbType with
      | d when d = DbType.Binary -> dbParam.SqlDbType <- SqlDbType.VarBinary
      | d when d = DbType.String -> dbParam.SqlDbType <- SqlDbType.NVarChar
      | d when d = DbType.StringFixedLength -> dbParam.SqlDbType <- SqlDbType.NChar
      | d when d = DbType.Date -> dbParam.SqlDbType <- SqlDbType.Date
      | d when d = DbType.DateTimeOffset -> dbParam.SqlDbType <- SqlDbType.DateTimeOffset
      | d when d = DbType.Time -> dbParam.SqlDbType <- SqlDbType.Time
      | _ -> 
        ()
    | _ ->
      ()

/// The dialect for Oracle Database 10g and higher.
type OracleDialect(?dataConvReg) = 
  inherit DialectBase(defaultArg dataConvReg (DataConvRegistry()))
 
  let escapeRegex = Regex(@"[$_%＿％]")

  override this.Name = "Oracle"

  override this.EscapeMetaChars(text) =
    escapeRegex.Replace(text, "$$$&")
 
  override this.PrepareSequenceSelect(sequenceName) = 
    let buf = SqlBuilder this
    buf.Append "select "
    buf.Append sequenceName
    buf.Append ".nextval from dual"
    buf.Build()
 
  override this.IsResultParamRecognizedAsOutputParam = true
 
  override this.IsUniqueConstraintViolation(exn:exn) = 
    match exn with
    | :? DbException as ex ->
      let prop = ex.GetType().GetProperty("Number", typeof<int>)
      prop <> null &&
      match prop.GetValue(ex, null) :?> int with
      | 1 -> true
      | _ -> false
    | _ -> false
 
  override this.CreateParamName (index:int) =
    ":p" + string index
 
  override this.CreateParamName (baseName:string) =
    baseName
 
  override this.BuildProcedureCallSql(procedureName, parameters) =
    let buf = StringBuilder 200
    buf +> procedureName +! " "
    parameters
    |> Seq.peek
    |> Seq.iter (fun (p, hasNext) ->
      buf 
      +> p.Name 
      +> " = " 
      +! (if p.Value = null then "null" else string p.Value)
      if hasNext then buf +! ", " )
    buf.ToString()
 
  override this.EncloseIdentifier(identifier) = "\"" + identifier + "\""

  override this.ConvertFromDbToUnderlyingClr (dbValue, destType) = 
    let (|ConvertibleToBoolean|_|) (dbValue:obj, destType) =
      if destType = typeof<bool> then
        match dbValue with
        | :? byte 
        | :? int16 
        | :? int32
        | :? int64 
        | :? decimal
        | :? uint16 
        | :? uint32 
        | :? uint64 
         ->
          try
            Some (box (Convert.ToInt32(dbValue) = 1))
          with
          | exn ->
            Some (box false)
        | _ ->
          Some (box false)
      else
        None
    let (|ConvertibleToTimeSpan|_|) (dbValue, destType) =
      if destType = typeof<TimeSpan> then
        let typ = dbValue.GetType()
        let assmebly = typ.Assembly
        if typ = assmebly.GetType("Oracle.DataAccess.Types.OracleIntervalDS") then
          let prop = typ.GetProperty("Value")
          Some (prop.GetValue(dbValue, null))
        else
          None
      else 
        None
    match dbValue, destType with
    | ConvertibleToBoolean(obj) 
    | ConvertibleToTimeSpan(obj) -> 
      obj
    | _ -> 
      base.ConvertFromDbToUnderlyingClr(dbValue, destType)
  
  override this.ConvertFromClrToDb (clrValue, srcType, udtTypeName) = 
    let dbValue, typ, dbType = base.ConvertFromClrToDb (clrValue, srcType, udtTypeName)
    match dbType with
    | d when d = DbType.Boolean -> 
      let dbValue =
        if Convert.IsDBNull dbValue then
          dbValue
        else 
          box (if true.Equals dbValue then 1 else 0)
      dbValue, typeof<int>, DbType.Int32
    | _ ->
      dbValue, typ, dbType
 
  override this.FormatAsSqlLiteral (dbValue:obj, clrType:Type, dbType:DbType) = 
    if Convert.IsDBNull dbValue then 
      "null"
    else
      let literal =
        match dbType with
        | d when d = DbType.String || d = DbType.StringFixedLength ->
            Some ("'" + string dbValue + "'")
        | d when d = DbType.Time ->
          match dbValue with
          | :? TimeSpan as t -> 
            Some ( "time '" + t.ToString("c") + "'")
          | _ -> 
            None
        | d when d = DbType.Date ->
          match dbValue with
          | :? DateTime as d -> 
            Some ("date '" + d.ToString("yyyy-MM-dd") + "'")
          | _ -> 
            None
        | d when d = DbType.DateTime ->
          match dbValue with
          | :? DateTime as d -> 
            Some ("timestamp '" + d.ToString("yyyy-MM-dd HH:mm:ss.fffffff") + "'")
          | _ -> 
            None
        | d when d = DbType.Object->
          match dbValue with
          | :? TimeSpan as d -> 
            Some (String.Format("interval '{0} {1}:{2}:{3}.{4}' day to second", d.ToString("dd"), d.ToString("hh"), d.ToString("mm"), d.ToString("ss"), d.ToString("fffffff")))
          | _ -> 
            None
        | _ ->
          None
      match literal with
      | Some value -> value
      | _ -> base.FormatAsSqlLiteral (dbValue, clrType, dbType)

  override this.SetupDbParam(param, dbParam) =
    base.SetupDbParam(param, dbParam)
    let dbParamType = dbParam.GetType()
    let assembly = dbParamType.Assembly
    let setOracleDbType typeName =
      let prop = dbParamType.GetProperty("OracleDbType")
      let enumType = assembly.GetType("Oracle.DataAccess.Client.OracleDbType")
      if prop <> null && enumType <> null then
        let enumValue = Enum.Parse(enumType, typeName)
        prop.SetValue(dbParam, enumValue, null)
    if param.Direction = Direction.Result then
      setOracleDbType "RefCursor"
    if param.DbType = DbType.Time then
      setOracleDbType "IntervalDS"
    if param.UdtTypeName <> null then
      let dbTypeName = if param.Type.IsArray then "Array" else "Object"
      setOracleDbType dbTypeName
      let prop = dbParamType.GetProperty("UdtTypeName")
      if prop <> null then
        prop.SetValue(dbParam, param.UdtTypeName, null)
  
  override this.MakeParamDisposer(command) = 
    { new IDisposable with 
      member this.Dispose() =
        let dispose : obj -> unit = function
        | :? IDisposable as d -> d.Dispose()
        | _ -> ()
        let count = command.Parameters.Count
        for i = 0 to count - 1 do
          let p = command.Parameters.[i]
          dispose p.Value
          dispose p }
