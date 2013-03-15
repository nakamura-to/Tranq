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

namespace Tranq.Test.MsSql

open System
open NUnit.Framework
open Tranq

module MappingTest = 

  type DefaultMapping =
    { [<Id>]
      DefaultMappingId : int32 
      ByteCol : byte
      Int16Col : int16
      Int32Col : int32
      Int64Col : int64
      BinaryCol : byte[]
      ImageBinaryCol : byte[]
      VarbinaryBinaryCol : byte[]
      [<Version(VersionKind.Computed)>]
      RowversionBinaryCol : byte[]
      BooleanCol : bool
      DateTimeCol : DateTime
      DateDateTimeCol : DateTime
      DateTime2Col : DateTime
      SmallDateTimeCol : DateTime
      DateTimeOffsetCol : DateTimeOffset
      DecimalCol : decimal
      NumericDecimalCol : decimal
      MoneyDecimalCol : decimal
      SmallMoneyDecimalCol : decimal
      DoubleCol : double
      SingleCol : single
      VarcharStringCol : string
      NVarcharStringCol : string
      NTextStringCol : string 
      GuidCol : Guid }

  [<Test>]
  let ``default mapping``() =
    let guid = Guid.NewGuid()
    Runner.rollbackOnly <| txRequired {
      do! Db.insert<DefaultMapping> {
        DefaultMappingId = 1 
        ByteCol = 3uy
        Int16Col = 123s
        Int32Col = (int32 Int16.MaxValue) * 2
        Int64Col = (int64 Int32.MaxValue) * 2L
        BinaryCol = [| 1uy; 2uy; 3uy; |]
        ImageBinaryCol = [| 1uy; 2uy; 3uy; |]
        VarbinaryBinaryCol = [| 1uy; 2uy; 3uy; |]
        RowversionBinaryCol = [| |]
        BooleanCol = true
        DateTimeCol = DateTime(2011, 4, 11, 13, 14, 15)
        DateDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15)
        DateTime2Col = DateTime(2011, 4, 11, 13, 14, 15)
        SmallDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15)
        DateTimeOffsetCol = DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15))
        DecimalCol = 123.456M
        NumericDecimalCol = 123.456M
        MoneyDecimalCol = 123.456M
        SmallMoneyDecimalCol = 123.456M
        DoubleCol = 123.5
        SingleCol = 123.5f
        VarcharStringCol = "abc"
        NVarcharStringCol = "あいう"
        NTextStringCol = "あいう" 
        GuidCol = guid } |> Tx.ignore
      return! Db.find<DefaultMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo 3uy
      ret.Int16Col |> isEqualTo 123s
      ret.Int32Col |> isEqualTo ((int32 Int16.MaxValue) * 2)
      ret.Int64Col |> isEqualTo ((int64 Int32.MaxValue) * 2L)
      ret.BinaryCol |> isEqualTo [|1uy; 2uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy|]
      ret.ImageBinaryCol |> isEqualTo [|1uy; 2uy; 3uy |]
      ret.VarbinaryBinaryCol |> isEqualTo [|1uy; 2uy; 3uy |]
      ret.RowversionBinaryCol |> isNotEqualTo Array.empty
      ret.DateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15))
      ret.DateDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 0, 0, 0))
      ret.DateTime2Col |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15))
      ret.SmallDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 0))
      ret.DateTimeOffsetCol |> isEqualTo (DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)))
      ret.DecimalCol |> isEqualTo 123.456M
      ret.NumericDecimalCol |> isEqualTo 123.456M
      ret.MoneyDecimalCol |> isEqualTo 123.456M
      ret.SmallMoneyDecimalCol |> isEqualTo 123.456M 
      ret.DoubleCol |> isEqualTo 123.5
      ret.SingleCol |> isEqualTo 123.5f
      ret.VarcharStringCol |> isEqualTo "abc" 
      ret.NVarcharStringCol |> isEqualTo "あいう" 
      ret.NTextStringCol |> isEqualTo "あいう"
      ret.GuidCol |> isEqualTo guid
    | Failure e -> 
      raise <| Exception("", e)

  [<Table(Name = "DefaultMapping")>]
  type OptionDefaultMapping =
    { [<Id>]
      DefaultMappingId : int32 
      ByteCol : byte option
      Int16Col : int16 option
      Int32Col : int32 option
      Int64Col : int64 option
      BinaryCol : byte[] option
      ImageBinaryCol : byte[] option
      VarbinaryBinaryCol : byte[] option
      [<Version(VersionKind.Computed)>]
      RowversionBinaryCol : byte[] option
      BooleanCol : bool option
      DateTimeCol : DateTime option
      DateDateTimeCol : DateTime option
      DateTime2Col : DateTime option
      SmallDateTimeCol : DateTime option
      DateTimeOffsetCol : DateTimeOffset option
      DecimalCol : decimal option
      NumericDecimalCol : decimal option
      MoneyDecimalCol : decimal option
      SmallMoneyDecimalCol : decimal option
      DoubleCol : double option
      SingleCol : single option
      VarcharStringCol : string option
      NVarcharStringCol : string option
      NTextStringCol : string option
      GuidCol : Guid option }

  [<Test>]
  let ``option mapping : some``() =
    let guid = Guid.NewGuid()
    Runner.rollbackOnly <| txRequired {
      do! Db.insert<OptionDefaultMapping> {
        DefaultMappingId = 1 
        ByteCol = Some 3uy
        Int16Col = Some 123s
        Int32Col = Some <| (int32 Int16.MaxValue) * 2
        Int64Col = Some <| (int64 Int32.MaxValue) * 2L
        BinaryCol = Some [| 1uy; 2uy; 3uy; |]
        ImageBinaryCol = Some [| 1uy; 2uy; 3uy; |]
        VarbinaryBinaryCol = Some [| 1uy; 2uy; 3uy; |]
        RowversionBinaryCol = Some [| |]
        BooleanCol = Some true
        DateTimeCol = Some <| DateTime(2011, 4, 11, 13, 14, 15)
        DateDateTimeCol = Some <| DateTime(2011, 4, 11, 13, 14, 15)
        DateTime2Col = Some <| DateTime(2011, 4, 11, 13, 14, 15)
        SmallDateTimeCol = Some <| DateTime(2011, 4, 11, 13, 14, 15)
        DateTimeOffsetCol = Some <| DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15))
        DecimalCol = Some 123.456M
        NumericDecimalCol = Some 123.456M
        MoneyDecimalCol = Some 123.456M
        SmallMoneyDecimalCol = Some 123.456M
        DoubleCol = Some 123.5
        SingleCol = Some 123.5f
        VarcharStringCol = Some "abc"
        NVarcharStringCol = Some "あいう"
        NTextStringCol = Some "あいう" 
        GuidCol = Some guid } |> Tx.ignore
      return! Db.find<OptionDefaultMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo (Some 3uy)
      ret.Int16Col |> isEqualTo (Some 123s)
      ret.Int32Col |> isEqualTo (Some <| (int32 Int16.MaxValue) * 2)
      ret.Int64Col |> isEqualTo (Some <| (int64 Int32.MaxValue) * 2L)
      ret.BinaryCol |> isEqualTo (Some [|1uy; 2uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy|])
      ret.ImageBinaryCol |> isEqualTo (Some [|1uy; 2uy; 3uy |])
      ret.VarbinaryBinaryCol |> isEqualTo (Some [|1uy; 2uy; 3uy |])
      ret.RowversionBinaryCol |> isNotEqualTo (Some Array.empty)
      ret.DateTimeCol |> isEqualTo (Some <| DateTime(2011, 4, 11, 13, 14, 15))
      ret.DateDateTimeCol |> isEqualTo (Some <| DateTime(2011, 4, 11, 0, 0, 0))
      ret.DateTime2Col |> isEqualTo (Some <| DateTime(2011, 4, 11, 13, 14, 15))
      ret.SmallDateTimeCol |> isEqualTo (Some <| DateTime(2011, 4, 11, 13, 14, 0))
      ret.DateTimeOffsetCol |> isEqualTo (Some <| DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)))
      ret.DecimalCol |> isEqualTo (Some 123.456M)
      ret.NumericDecimalCol |> isEqualTo (Some 123.456M)
      ret.MoneyDecimalCol |> isEqualTo (Some 123.456M)
      ret.SmallMoneyDecimalCol |> isEqualTo (Some 123.456M)
      ret.DoubleCol |> isEqualTo (Some 123.5)
      ret.SingleCol |> isEqualTo (Some 123.5f)
      ret.VarcharStringCol |> isEqualTo (Some "abc")
      ret.NVarcharStringCol |> isEqualTo (Some "あいう")
      ret.NTextStringCol |> isEqualTo (Some "あいう")
      ret.GuidCol |> isEqualTo (Some guid)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``option mapping : none``() =
    Runner.rollbackOnly <| txRequired {
      do! Db.insert<OptionDefaultMapping> {
        DefaultMappingId = 1 
        ByteCol = None
        Int16Col = None
        Int32Col = None
        Int64Col = None
        BinaryCol = None
        ImageBinaryCol = None
        VarbinaryBinaryCol = None
        RowversionBinaryCol = None
        BooleanCol = None
        DateTimeCol = None
        DateDateTimeCol = None
        DateTime2Col = None
        SmallDateTimeCol = None
        DateTimeOffsetCol = None
        DecimalCol = None
        NumericDecimalCol = None
        MoneyDecimalCol = None
        SmallMoneyDecimalCol = None
        DoubleCol = None
        SingleCol = None
        VarcharStringCol = None
        NVarcharStringCol = None
        NTextStringCol = None
        GuidCol = None } |> Tx.ignore
      return! Db.find<OptionDefaultMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo None
      ret.Int16Col |> isEqualTo None
      ret.Int32Col |> isEqualTo None
      ret.Int64Col |> isEqualTo None
      ret.BinaryCol |> isEqualTo None
      ret.ImageBinaryCol |> isEqualTo None
      ret.VarbinaryBinaryCol |> isEqualTo None
      ret.RowversionBinaryCol |> isNotEqualTo None
      ret.DateTimeCol |> isEqualTo None
      ret.DateDateTimeCol |> isEqualTo None
      ret.DateTime2Col |> isEqualTo None
      ret.SmallDateTimeCol |> isEqualTo None
      ret.DateTimeOffsetCol |> isEqualTo None
      ret.DecimalCol |> isEqualTo None
      ret.NumericDecimalCol |> isEqualTo None
      ret.MoneyDecimalCol |> isEqualTo None
      ret.SmallMoneyDecimalCol |> isEqualTo None
      ret.DoubleCol |> isEqualTo None
      ret.SingleCol |> isEqualTo None
      ret.VarcharStringCol |> isEqualTo None
      ret.NVarcharStringCol |> isEqualTo None
      ret.NTextStringCol |> isEqualTo None
      ret.GuidCol |> isEqualTo None
    | Failure e -> 
      raise <| Exception("", e)

  type TimeSpanMapping =
    { [<Id>]
      TimeSpanMappingId : int32 
      TimeSpanCol : TimeSpan }

  [<Test>]
  let ``timespan mapping``() =
    Runner.rollbackOnly <| txRequired { 
      do! Db.insert<TimeSpanMapping> { 
        TimeSpanMappingId = 1 
        TimeSpanCol = TimeSpan(13, 14, 15) } |> Tx.ignore
      return! Db.find<TimeSpanMapping> [1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo 
        { TimeSpanMappingId = 1
          TimeSpanCol = TimeSpan(13, 14, 15)}
    | Failure e -> 
      raise <| Exception("", e)

  [<Table(Name = "TimeSpanMapping")>]
  type OptionTimeSpanMapping =
    { [<Id>]
      TimeSpanMappingId : int32 
      TimeSpanCol : TimeSpan option }

  [<Test>]
  let ``option timespan mapping : some``() =
    Runner.rollbackOnly <| txRequired { 
      do! Db.insert<OptionTimeSpanMapping> { 
        TimeSpanMappingId = 1 
        TimeSpanCol = Some <| TimeSpan(13, 14, 15) } |> Tx.ignore
      return! Db.find<OptionTimeSpanMapping> [1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo 
        { TimeSpanMappingId = 1
          TimeSpanCol = Some <| TimeSpan(13, 14, 15)}
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``option timespan mapping : none``() =
    Runner.rollbackOnly <| txRequired { 
      do! Db.insert<OptionTimeSpanMapping> { 
        TimeSpanMappingId = 1 
        TimeSpanCol = None } |> Tx.ignore
      return! Db.find<OptionTimeSpanMapping> [1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo {
        TimeSpanMappingId = 1
        TimeSpanCol = None }
    | Failure e -> 
      raise <| Exception("", e)

  type LobMapping =
    { [<Id>]
      LobMappingId : int32 
      VarBinaryCol : byte[]
      VarcharStringCol : string
      NVarcharStringCol : string }

  [<Test>]
  let ``lob mapping``() =
    Runner.rollbackOnly <| txRequired { 
      do! Db.insert<LobMapping> { 
        LobMappingId = 1 
        VarBinaryCol = [| 1uy; 2uy; 3uy; |]
        VarcharStringCol = "abc"
        NVarcharStringCol = "あいう" } |> Tx.ignore
      return! Db.find<LobMapping> [1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo { 
        LobMappingId = 1 
        VarBinaryCol = [| 1uy; 2uy; 3uy; |]
        VarcharStringCol = "abc"
        NVarcharStringCol = "あいう" }
    | Failure e -> 
      raise <| Exception("", e)

  module Byte =
    type t = Wrapper of byte
    let conv = { new IDataConv<t, byte> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int16 =
    type t = Wrapper of int16
    let conv = { new IDataConv<t, int16> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int32 =
    type t = Wrapper of int32
    let conv = { new IDataConv<t, int32> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int64 =
    type t = Wrapper of int64
    let conv = { new IDataConv<t, int64> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Bytes =
    type t = Wrapper of byte[]
    let conv = { new IDataConv<t, byte[]> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Bool =
    type t = Wrapper of bool
    let conv = { new IDataConv<t, bool> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DateTime =
    type t = Wrapper of DateTime
    let conv = { new IDataConv<t, DateTime> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DateTimeOffset =
    type t = Wrapper of DateTimeOffset
    let conv = { new IDataConv<t, DateTimeOffset> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Decimal =
    type t = Wrapper of decimal
    let conv = { new IDataConv<t, decimal> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Double =
    type t = Wrapper of double
    let conv = { new IDataConv<t, double> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Single =
    type t = Wrapper of single
    let conv = { new IDataConv<t, single> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module String =
    type t = Wrapper of string
    let conv = { new IDataConv<t, string> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Guid =
    type t = Wrapper of Guid
    let conv = { new IDataConv<t, Guid> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  open System.Data.SqlClient
  open System.Data.Common

  let config = 
    let reg = DataConvRegistry()
    reg.Add(Byte.conv)
    reg.Add(Int16.conv)
    reg.Add(Int32.conv)
    reg.Add(Int64.conv)
    reg.Add(Bytes.conv)
    reg.Add(Bool.conv)
    reg.Add(DateTime.conv)
    reg.Add(DateTimeOffset.conv)
    reg.Add(Decimal.conv)
    reg.Add(Double.conv)
    reg.Add(Single.conv)
    reg.Add(String.conv)
    reg.Add(Guid.conv)
    { Runner.config with Dialect = MsSqlDialect(reg) }

  let rollbackOnly tx =
    let tx = txRequired {
      do! Tx.rollbackOnly
      return! tx }
    Tx.eval config tx

  [<Table(Name = "DefaultMapping")>]
  type RichMapping =
    { [<Id>]
      DefaultMappingId : int32 
      ByteCol : Byte.t
      Int16Col : Int16.t
      Int32Col : Int32.t
      Int64Col : Int64.t
      BinaryCol : Bytes.t
      ImageBinaryCol : Bytes.t
      VarbinaryBinaryCol : Bytes.t
      [<Version(VersionKind.Computed)>]
      RowversionBinaryCol : Bytes.t
      BooleanCol : Bool.t
      DateTimeCol : DateTime.t
      DateDateTimeCol : DateTime.t
      DateTime2Col : DateTime.t
      SmallDateTimeCol : DateTime.t
      DateTimeOffsetCol : DateTimeOffset.t
      DecimalCol : Decimal.t
      NumericDecimalCol : Decimal.t
      MoneyDecimalCol : Decimal.t
      SmallMoneyDecimalCol : Decimal.t
      DoubleCol : Double.t
      SingleCol : Single.t
      VarcharStringCol : String.t
      NVarcharStringCol : String.t
      NTextStringCol : String.t
      GuidCol : Guid.t }

  [<Test>]
  let ``rich mapping``() =
    let guid = Guid.NewGuid()
    rollbackOnly <| txRequired {
      do! Db.insert<RichMapping> {
        DefaultMappingId = 1 
        ByteCol = 3uy |> Byte.Wrapper
        Int16Col = 123s |> Int16.Wrapper
        Int32Col = (int32 Int16.MaxValue) * 2 |> Int32.Wrapper
        Int64Col = (int64 Int32.MaxValue) * 2L |> Int64.Wrapper
        BinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper
        ImageBinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper
        VarbinaryBinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper
        RowversionBinaryCol = [| |] |> Bytes.Wrapper
        BooleanCol = true |> Bool.Wrapper
        DateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper
        DateDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper
        DateTime2Col = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper
        SmallDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper
        DateTimeOffsetCol = DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> DateTimeOffset.Wrapper
        DecimalCol = 123.456M |> Decimal.Wrapper
        NumericDecimalCol = 123.456M |> Decimal.Wrapper
        MoneyDecimalCol = 123.456M |> Decimal.Wrapper
        SmallMoneyDecimalCol = 123.456M |> Decimal.Wrapper
        DoubleCol = 123.5 |> Double.Wrapper
        SingleCol = 123.5f |> Single.Wrapper
        VarcharStringCol = "abc" |> String.Wrapper
        NVarcharStringCol = "あいう" |> String.Wrapper
        NTextStringCol = "あいう" |> String.Wrapper
        GuidCol = guid |> Guid.Wrapper } |> Tx.ignore
      return! Db.find<RichMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo (3uy |> Byte.Wrapper)
      ret.Int16Col |> isEqualTo (123s |> Int16.Wrapper)
      ret.Int32Col |> isEqualTo ((int32 Int16.MaxValue) * 2 |> Int32.Wrapper)
      ret.Int64Col |> isEqualTo ((int64 Int32.MaxValue) * 2L |> Int64.Wrapper)
      ret.BinaryCol |> isEqualTo ([|1uy; 2uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy|] |> Bytes.Wrapper)
      ret.ImageBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Bytes.Wrapper)
      ret.VarbinaryBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Bytes.Wrapper)
      ret.RowversionBinaryCol |> isNotEqualTo (Array.empty |> Bytes.Wrapper)
      ret.DateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper)
      ret.DateDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 0, 0, 0) |> DateTime.Wrapper)
      ret.DateTime2Col |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper)
      ret.SmallDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 0) |> DateTime.Wrapper)
      ret.DateTimeOffsetCol |> isEqualTo (DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> DateTimeOffset.Wrapper)
      ret.DecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper)
      ret.NumericDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper)
      ret.MoneyDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper)
      ret.SmallMoneyDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper)
      ret.DoubleCol |> isEqualTo (123.5 |> Double.Wrapper)
      ret.SingleCol |> isEqualTo (123.5f |> Single.Wrapper)
      ret.VarcharStringCol |> isEqualTo ("abc" |> String.Wrapper)
      ret.NVarcharStringCol |> isEqualTo ("あいう" |> String.Wrapper)
      ret.NTextStringCol |> isEqualTo ("あいう" |> String.Wrapper)
      ret.GuidCol |> isEqualTo (guid |> Guid.Wrapper)
    | Failure e -> 
      raise <| Exception("", e)

  [<Table(Name = "DefaultMapping")>]
  type OptionRichMapping =
    { [<Id>]
      DefaultMappingId : int32
      ByteCol : Byte.t option
      Int16Col : Int16.t option
      Int32Col : Int32.t option
      Int64Col : Int64.t option
      BinaryCol : Bytes.t option
      ImageBinaryCol : Bytes.t option
      VarbinaryBinaryCol : Bytes.t option
      [<Version(VersionKind.Computed)>]
      RowversionBinaryCol : Bytes.t option
      BooleanCol : Bool.t option
      DateTimeCol : DateTime.t option
      DateDateTimeCol : DateTime.t option
      DateTime2Col : DateTime.t option
      SmallDateTimeCol : DateTime.t option
      DateTimeOffsetCol : DateTimeOffset.t option
      DecimalCol : Decimal.t option
      NumericDecimalCol : Decimal.t option
      MoneyDecimalCol : Decimal.t option
      SmallMoneyDecimalCol : Decimal.t option
      DoubleCol : Double.t option
      SingleCol : Single.t option
      VarcharStringCol : String.t option
      NVarcharStringCol : String.t option
      NTextStringCol : String.t option
      GuidCol : Guid.t option }

  [<Test>]
  let ``option rich mapping : some``() =
    let guid = Guid.NewGuid()
    rollbackOnly <| txRequired {
      do! Db.insert<OptionRichMapping> {
        DefaultMappingId = 1 
        ByteCol = 3uy |> Byte.Wrapper |> Some
        Int16Col = 123s |> Int16.Wrapper |> Some
        Int32Col = (int32 Int16.MaxValue) * 2 |> Int32.Wrapper |> Some
        Int64Col = (int64 Int32.MaxValue) * 2L |> Int64.Wrapper |> Some
        BinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper |> Some
        ImageBinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper |> Some
        VarbinaryBinaryCol = [| 1uy; 2uy; 3uy; |] |> Bytes.Wrapper |> Some
        RowversionBinaryCol = [| |] |> Bytes.Wrapper |> Some
        BooleanCol = true |> Bool.Wrapper |> Some
        DateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some
        DateDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some
        DateTime2Col = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some
        SmallDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some
        DateTimeOffsetCol = DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> DateTimeOffset.Wrapper |> Some
        DecimalCol = 123.456M |> Decimal.Wrapper |> Some
        NumericDecimalCol = 123.456M |> Decimal.Wrapper |> Some
        MoneyDecimalCol = 123.456M |> Decimal.Wrapper |> Some
        SmallMoneyDecimalCol = 123.456M |> Decimal.Wrapper |> Some
        DoubleCol = 123.5 |> Double.Wrapper |> Some
        SingleCol = 123.5f |> Single.Wrapper |> Some
        VarcharStringCol = "abc" |> String.Wrapper |> Some
        NVarcharStringCol = "あいう" |> String.Wrapper |> Some
        NTextStringCol = "あいう" |> String.Wrapper |> Some
        GuidCol = guid |> Guid.Wrapper |> Some } |> Tx.ignore
      return! Db.find<OptionRichMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo (3uy |> Byte.Wrapper |> Some)
      ret.Int16Col |> isEqualTo (123s |> Int16.Wrapper |> Some)
      ret.Int32Col |> isEqualTo ((int32 Int16.MaxValue) * 2 |> Int32.Wrapper |> Some)
      ret.Int64Col |> isEqualTo ((int64 Int32.MaxValue) * 2L |> Int64.Wrapper |> Some)
      ret.BinaryCol |> isEqualTo ([|1uy; 2uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy|] |> Bytes.Wrapper |> Some)
      ret.ImageBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Bytes.Wrapper |> Some)
      ret.VarbinaryBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Bytes.Wrapper |> Some)
      ret.RowversionBinaryCol |> isNotEqualTo (Array.empty |> Bytes.Wrapper |> Some)
      ret.DateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some)
      ret.DateDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 0, 0, 0) |> DateTime.Wrapper |> Some)
      ret.DateTime2Col |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> DateTime.Wrapper |> Some)
      ret.SmallDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 0) |> DateTime.Wrapper |> Some)
      ret.DateTimeOffsetCol |> isEqualTo (DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> DateTimeOffset.Wrapper |> Some)
      ret.DecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper |> Some)
      ret.NumericDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper |> Some)
      ret.MoneyDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper |> Some)
      ret.SmallMoneyDecimalCol |> isEqualTo (123.456M |> Decimal.Wrapper |> Some)
      ret.DoubleCol |> isEqualTo (123.5 |> Double.Wrapper |> Some)
      ret.SingleCol |> isEqualTo (123.5f |> Single.Wrapper |> Some)
      ret.VarcharStringCol |> isEqualTo ("abc" |> String.Wrapper |> Some)
      ret.NVarcharStringCol |> isEqualTo ("あいう" |> String.Wrapper |> Some)
      ret.NTextStringCol |> isEqualTo ("あいう" |> String.Wrapper |> Some)
      ret.GuidCol |> isEqualTo (guid |> Guid.Wrapper |> Some)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``option rich mapping : none``() =
    let guid = Guid.NewGuid()
    rollbackOnly <| txRequired {
      do! Db.insert<OptionRichMapping> {
        DefaultMappingId = 1 
        ByteCol = None
        Int16Col = None
        Int32Col = None
        Int64Col = None
        BinaryCol = None
        ImageBinaryCol = None
        VarbinaryBinaryCol = None
        RowversionBinaryCol = None
        BooleanCol = None
        DateTimeCol = None
        DateDateTimeCol = None
        DateTime2Col = None
        SmallDateTimeCol = None
        DateTimeOffsetCol = None
        DecimalCol = None
        NumericDecimalCol = None
        MoneyDecimalCol = None
        SmallMoneyDecimalCol = None
        DoubleCol = None
        SingleCol = None
        VarcharStringCol = None
        NVarcharStringCol = None
        NTextStringCol = None
        GuidCol = None } |> Tx.ignore
      return! Db.find<OptionRichMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo None
      ret.Int16Col |> isEqualTo None
      ret.Int32Col |> isEqualTo None
      ret.Int64Col |> isEqualTo None
      ret.BinaryCol |> isEqualTo None
      ret.ImageBinaryCol |> isEqualTo None
      ret.VarbinaryBinaryCol |> isEqualTo None
      ret.RowversionBinaryCol |> isNotEqualTo None
      ret.DateTimeCol |> isEqualTo None
      ret.DateDateTimeCol |> isEqualTo None
      ret.DateTime2Col |> isEqualTo None
      ret.SmallDateTimeCol |> isEqualTo None
      ret.DateTimeOffsetCol |> isEqualTo None
      ret.DecimalCol |> isEqualTo None
      ret.NumericDecimalCol |> isEqualTo None
      ret.MoneyDecimalCol |> isEqualTo None
      ret.SmallMoneyDecimalCol |> isEqualTo None
      ret.DoubleCol |> isEqualTo None
      ret.SingleCol |> isEqualTo None
      ret.VarcharStringCol |> isEqualTo None
      ret.NVarcharStringCol |> isEqualTo None
      ret.NTextStringCol |> isEqualTo None
      ret.GuidCol |> isEqualTo None
    | Failure e -> 
      raise <| Exception("", e)

  module ByteOpt =
    type t = Wrapper of byte option
    let conv = { new IDataConv<t, byte option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int16Opt =
    type t = Wrapper of int16 option
    let conv = { new IDataConv<t, int16 option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int32Opt =
    type t = Wrapper of int32 option
    let conv = { new IDataConv<t, int32 option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module Int64Opt =
    type t = Wrapper of int64 option
    let conv = { new IDataConv<t, int64 option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module BytesOpt =
    type t = Wrapper of byte[] option
    let conv = { new IDataConv<t, byte[] option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module BoolOpt =
    type t = Wrapper of bool option
    let conv = { new IDataConv<t, bool option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DateTimeOpt =
    type t = Wrapper of DateTime option
    let conv = { new IDataConv<t, DateTime option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DateTimeOffsetOpt =
    type t = Wrapper of DateTimeOffset option
    let conv = { new IDataConv<t, DateTimeOffset option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DecimalOpt =
    type t = Wrapper of decimal option
    let conv = { new IDataConv<t, decimal option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module DoubleOpt =
    type t = Wrapper of double option
    let conv = { new IDataConv<t, double option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module SingleOpt =
    type t = Wrapper of single option
    let conv = { new IDataConv<t, single option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module StringOpt =
    type t = Wrapper of string option
    let conv = { new IDataConv<t, string option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  module GuidOpt =
    type t = Wrapper of Guid option
    let conv = { new IDataConv<t, Guid option> with
      member this.Compose(value) = Wrapper value
      member this.Decompose(Wrapper(value)) = value }

  let configOpt = 
    let reg = DataConvRegistry()
    reg.Add(ByteOpt.conv)
    reg.Add(Int16Opt.conv)
    reg.Add(Int32Opt.conv)
    reg.Add(Int64Opt.conv)
    reg.Add(BytesOpt.conv)
    reg.Add(BoolOpt.conv)
    reg.Add(DateTimeOpt.conv)
    reg.Add(DateTimeOffsetOpt.conv)
    reg.Add(DecimalOpt.conv)
    reg.Add(DoubleOpt.conv)
    reg.Add(SingleOpt.conv)
    reg.Add(StringOpt.conv)
    reg.Add(GuidOpt.conv)
    { Runner.config with Dialect = MsSqlDialect(reg) }

  let rollbackOnlyOpt tx =
    let tx = txRequired {
      do! Tx.rollbackOnly
      return! tx }
    Tx.eval configOpt tx

  [<Table(Name = "DefaultMapping")>]
  type RichOptMapping =
    { [<Id>]
      DefaultMappingId : int32
      ByteCol : ByteOpt.t
      Int16Col : Int16Opt.t
      Int32Col : Int32Opt.t
      Int64Col : Int64Opt.t
      BinaryCol : BytesOpt.t
      ImageBinaryCol : BytesOpt.t
      VarbinaryBinaryCol : BytesOpt.t
      [<Version(VersionKind.Computed)>]
      RowversionBinaryCol : BytesOpt.t
      BooleanCol : BoolOpt.t
      DateTimeCol : DateTimeOpt.t
      DateDateTimeCol : DateTimeOpt.t
      DateTime2Col : DateTimeOpt.t
      SmallDateTimeCol : DateTimeOpt.t
      DateTimeOffsetCol : DateTimeOffsetOpt.t
      DecimalCol : DecimalOpt.t
      NumericDecimalCol : DecimalOpt.t
      MoneyDecimalCol : DecimalOpt.t
      SmallMoneyDecimalCol : DecimalOpt.t
      DoubleCol : DoubleOpt.t
      SingleCol : SingleOpt.t
      VarcharStringCol : StringOpt.t
      NVarcharStringCol : StringOpt.t
      NTextStringCol : StringOpt.t
      GuidCol : GuidOpt.t }

  [<Test>]
  let ``rich opt mapping : some``() =
    let guid = Guid.NewGuid()
    rollbackOnlyOpt <| txRequired {
      do! Db.insert<RichOptMapping> {
        DefaultMappingId = 1 
        ByteCol = 3uy |> Some |> ByteOpt.Wrapper
        Int16Col = 123s |> Some |> Int16Opt.Wrapper
        Int32Col = (int32 Int16.MaxValue) * 2 |> Some |> Int32Opt.Wrapper
        Int64Col = (int64 Int32.MaxValue) * 2L |> Some |> Int64Opt.Wrapper
        BinaryCol = [| 1uy; 2uy; 3uy; |] |> Some |> BytesOpt.Wrapper
        ImageBinaryCol = [| 1uy; 2uy; 3uy; |] |> Some |> BytesOpt.Wrapper
        VarbinaryBinaryCol = [| 1uy; 2uy; 3uy; |] |> Some |> BytesOpt.Wrapper
        RowversionBinaryCol = [| |] |> Some |> BytesOpt.Wrapper
        BooleanCol = true |> Some |> BoolOpt.Wrapper
        DateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper
        DateDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper
        DateTime2Col = DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper
        SmallDateTimeCol = DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper
        DateTimeOffsetCol = DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> Some |> DateTimeOffsetOpt.Wrapper
        DecimalCol = 123.456M |> Some |> DecimalOpt.Wrapper
        NumericDecimalCol = 123.456M |> Some |> DecimalOpt.Wrapper
        MoneyDecimalCol = 123.456M |> Some |> DecimalOpt.Wrapper
        SmallMoneyDecimalCol = 123.456M |> Some |> DecimalOpt.Wrapper
        DoubleCol = 123.5 |> Some |> DoubleOpt.Wrapper
        SingleCol = 123.5f |> Some |> SingleOpt.Wrapper
        VarcharStringCol = "abc" |> Some |> StringOpt.Wrapper
        NVarcharStringCol = "あいう" |> Some |> StringOpt.Wrapper
        NTextStringCol = "あいう" |> Some |> StringOpt.Wrapper
        GuidCol = guid |> Some |> GuidOpt.Wrapper } |> Tx.ignore
      return! Db.find<RichOptMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo (3uy |> Some |> ByteOpt.Wrapper)
      ret.Int16Col |> isEqualTo (123s |> Some |> Int16Opt.Wrapper)
      ret.Int32Col |> isEqualTo ((int32 Int16.MaxValue) * 2 |> Some |> Int32Opt.Wrapper)
      ret.Int64Col |> isEqualTo ((int64 Int32.MaxValue) * 2L |> Some |> Int64Opt.Wrapper)
      ret.BinaryCol |> isEqualTo ([|1uy; 2uy; 3uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy|] |> Some |> BytesOpt.Wrapper)
      ret.ImageBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Some |> BytesOpt.Wrapper)
      ret.VarbinaryBinaryCol |> isEqualTo ([|1uy; 2uy; 3uy |] |> Some |> BytesOpt.Wrapper)
      ret.RowversionBinaryCol |> isNotEqualTo (Array.empty |> Some |> BytesOpt.Wrapper)
      ret.DateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper)
      ret.DateDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 0, 0, 0) |> Some |> DateTimeOpt.Wrapper)
      ret.DateTime2Col |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 15) |> Some |> DateTimeOpt.Wrapper)
      ret.SmallDateTimeCol |> isEqualTo (DateTime(2011, 4, 11, 13, 14, 0) |> Some |> DateTimeOpt.Wrapper)
      ret.DateTimeOffsetCol |> isEqualTo (DateTimeOffset(DateTime(2011, 4, 11, 13, 14, 15)) |> Some |> DateTimeOffsetOpt.Wrapper)
      ret.DecimalCol |> isEqualTo (123.456M |> Some |> DecimalOpt.Wrapper)
      ret.NumericDecimalCol |> isEqualTo (123.456M |> Some |> DecimalOpt.Wrapper)
      ret.MoneyDecimalCol |> isEqualTo (123.456M |> Some |> DecimalOpt.Wrapper)
      ret.SmallMoneyDecimalCol |> isEqualTo (123.456M |> Some |> DecimalOpt.Wrapper)
      ret.DoubleCol |> isEqualTo (123.5 |> Some |> DoubleOpt.Wrapper)
      ret.SingleCol |> isEqualTo (123.5f |> Some |> SingleOpt.Wrapper)
      ret.VarcharStringCol |> isEqualTo ("abc" |> Some |> StringOpt.Wrapper)
      ret.NVarcharStringCol |> isEqualTo ("あいう" |> Some |> StringOpt.Wrapper)
      ret.NTextStringCol |> isEqualTo ("あいう" |> Some |> StringOpt.Wrapper)
      ret.GuidCol |> isEqualTo (guid |> Some |> GuidOpt.Wrapper)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``rich opt mapping : noe``() =
    let guid = Guid.NewGuid()
    rollbackOnlyOpt <| txRequired {
      do! Db.insert<RichOptMapping> {
        DefaultMappingId = 1 
        ByteCol = None |> ByteOpt.Wrapper
        Int16Col = None |> Int16Opt.Wrapper
        Int32Col = None |> Int32Opt.Wrapper
        Int64Col = None |> Int64Opt.Wrapper
        BinaryCol = None |> BytesOpt.Wrapper
        ImageBinaryCol = None|> BytesOpt.Wrapper
        VarbinaryBinaryCol = None |> BytesOpt.Wrapper
        RowversionBinaryCol = None |> BytesOpt.Wrapper
        BooleanCol = None |> BoolOpt.Wrapper
        DateTimeCol = None |> DateTimeOpt.Wrapper
        DateDateTimeCol = None |> DateTimeOpt.Wrapper
        DateTime2Col = None |> DateTimeOpt.Wrapper
        SmallDateTimeCol = None |> DateTimeOpt.Wrapper
        DateTimeOffsetCol = None |> DateTimeOffsetOpt.Wrapper
        DecimalCol = None |> DecimalOpt.Wrapper
        NumericDecimalCol = None |> DecimalOpt.Wrapper
        MoneyDecimalCol = None |> DecimalOpt.Wrapper
        SmallMoneyDecimalCol = None |> DecimalOpt.Wrapper
        DoubleCol = None |> DoubleOpt.Wrapper
        SingleCol = None |> SingleOpt.Wrapper
        VarcharStringCol =  None |> StringOpt.Wrapper
        NVarcharStringCol = None |> StringOpt.Wrapper
        NTextStringCol = None |> StringOpt.Wrapper
        GuidCol = None |> GuidOpt.Wrapper } |> Tx.ignore
      return! Db.find<RichOptMapping> [1] }
    |> function
    | Success ret -> 
      ret.ByteCol |> isEqualTo (None |> ByteOpt.Wrapper)
      ret.Int16Col |> isEqualTo (None |> Int16Opt.Wrapper)
      ret.Int32Col |> isEqualTo (None |> Int32Opt.Wrapper)
      ret.Int64Col |> isEqualTo (None |> Int64Opt.Wrapper)
      ret.BinaryCol |> isEqualTo (None |> BytesOpt.Wrapper)
      ret.ImageBinaryCol |> isEqualTo (None |> BytesOpt.Wrapper)
      ret.VarbinaryBinaryCol |> isEqualTo (None |> BytesOpt.Wrapper)
      ret.RowversionBinaryCol |> isNotEqualTo (None |> BytesOpt.Wrapper)
      ret.DateTimeCol |> isEqualTo (None |> DateTimeOpt.Wrapper)
      ret.DateDateTimeCol |> isEqualTo (None |> DateTimeOpt.Wrapper)
      ret.DateTime2Col |> isEqualTo (None |> DateTimeOpt.Wrapper)
      ret.SmallDateTimeCol |> isEqualTo (None |> DateTimeOpt.Wrapper)
      ret.DateTimeOffsetCol |> isEqualTo (None |> DateTimeOffsetOpt.Wrapper)
      ret.DecimalCol |> isEqualTo (None |> DecimalOpt.Wrapper)
      ret.NumericDecimalCol |> isEqualTo (None |> DecimalOpt.Wrapper)
      ret.MoneyDecimalCol |> isEqualTo (None |> DecimalOpt.Wrapper)
      ret.SmallMoneyDecimalCol |> isEqualTo (None |> DecimalOpt.Wrapper)
      ret.DoubleCol |> isEqualTo (None |> DoubleOpt.Wrapper)
      ret.SingleCol |> isEqualTo (None |> SingleOpt.Wrapper)
      ret.VarcharStringCol |> isEqualTo (None |> StringOpt.Wrapper)
      ret.NVarcharStringCol |> isEqualTo (None |> StringOpt.Wrapper)
      ret.NTextStringCol |> isEqualTo (None |> StringOpt.Wrapper)
      ret.GuidCol |> isEqualTo (None |> GuidOpt.Wrapper)
    | Failure e -> 
      raise <| Exception("", e)