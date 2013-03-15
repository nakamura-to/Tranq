namespace Tranq.GlobalTx

open System.Transactions
open Tranq

type GlobalTxBuilder(txAttr: TxAttr, txIsolatioinLevel: TxIsolationLevel, ?config: TxConfig) = 
  inherit TxBuilder(TxAttr.Required, TxIsolationLevel.ReadCommitted)
  let primaryConfig = config
  let isolationLevel = function
    | ReadUncommitted -> System.Transactions.IsolationLevel.ReadUncommitted
    | ReadCommitted -> System.Transactions.IsolationLevel.ReadCommitted
    | RepeatableRead -> System.Transactions.IsolationLevel.RepeatableRead
    | Serializable -> System.Transactions.IsolationLevel.Serializable
    | Snapshot -> System.Transactions.IsolationLevel.Snapshot
  override this.Run(f) = Tx(fun ({Config = config; Connection = con; Transaction = tx; State = state} as ctx) ->
    let config = match primaryConfig with Some x -> x | _ -> config
    let listener = config.Listener
    let runCore ctx = Tx.runCore f ctx
    let notifyBegin txInfo =
      config.Listener (TxBegun (txInfo, txAttr, txIsolatioinLevel))
    let notifyCommit txInfo =
      config.Listener (TxCommitted (txInfo, txAttr, txIsolatioinLevel))
    let notifyRollback txInfo = 
      config.Listener (TxRolledback (txInfo, txAttr, txIsolatioinLevel))
    let completeTx logging (txScope: TransactionScope) txInfo (result, state) =
      match result with
      | Success value -> 
        try
          if state.IsRollbackOnly then
            if logging then notifyRollback txInfo
          else
            txScope.Complete()
            if logging then notifyCommit txInfo
          Success value, state
        with e ->
          Failure e, state
      | Failure exn ->
        try notifyRollback txInfo with e -> ()
        Failure exn, state 
    match txAttr, tx with
    | Supports, _ ->
      runCore ctx
    | Required, _ ->
      let alreadyExists = Transaction.Current <> null
      let scopeOption = TransactionScopeOption.Required
      let transactionOptions = TransactionOptions(IsolationLevel = isolationLevel txIsolatioinLevel)
      use txScope = new TransactionScope(scopeOption, transactionOptions)
      use con = config.ConnectionProvider()
      let info = Transaction.Current.TransactionInformation
      let txInfo = { LocalId = info.LocalIdentifier; GlobalId = info.DistributedIdentifier }
      if alreadyExists then
        runCore {
          ctx with 
            Connection = con
            Transaction = None
            TransactionInfo = None
            State = state}
        |> completeTx false txScope txInfo
      else
        notifyBegin txInfo
        runCore {
          ctx with
            Connection = con
            Transaction = None
            TransactionInfo = Some txInfo
            State = {IsRollbackOnly = false}}
        |> completeTx true txScope txInfo
    | RequiresNew, _ ->
      let scopeOption = TransactionScopeOption.RequiresNew
      let transactionOptions = TransactionOptions(IsolationLevel = isolationLevel txIsolatioinLevel)
      use txScope = new TransactionScope(scopeOption, transactionOptions)
      use con = config.ConnectionProvider()
      let info = Transaction.Current.TransactionInformation
      let txInfo = { LocalId = info.LocalIdentifier; GlobalId = info.DistributedIdentifier }
      notifyBegin txInfo
      runCore {
        ctx with
          Connection = con
          Transaction = None
          TransactionInfo = Some txInfo
          State = {IsRollbackOnly = false}}
      |> completeTx true txScope txInfo
    | NotSupported, _ -> 
      let scopeOption = TransactionScopeOption.Suppress
      let transactionOptions = TransactionOptions(IsolationLevel = isolationLevel txIsolatioinLevel)
      use tx = new TransactionScope(scopeOption, transactionOptions)
      use con = config.ConnectionProvider()
      runCore {
        ctx with
          Connection = con
          Transaction = None
          TransactionInfo = None
          State = {IsRollbackOnly = false}})
    default this.With(newTxIsolatioinLevel) = 
      GlobalTxBuilder(txAttr, newTxIsolatioinLevel) :> TxBuilder
    member this.With(config) =
      GlobalTxBuilder(txAttr, txIsolatioinLevel, config) :> TxBuilder
    member this.With(newTxIsolatioinLevel, config) =
      GlobalTxBuilder(txAttr, newTxIsolatioinLevel, config) :> TxBuilder

[<AutoOpen>]
module Directives =

  let internal defaultIsolationLevel = TxIsolationLevel.ReadCommitted

  let txRequired = GlobalTxBuilder(TxAttr.Required, defaultIsolationLevel)

  let txRequiresNew = GlobalTxBuilder(TxAttr.RequiresNew, defaultIsolationLevel)

  let txSupports = GlobalTxBuilder(TxAttr.Supports, defaultIsolationLevel)

  let txNotSupported = GlobalTxBuilder(TxAttr.NotSupported, defaultIsolationLevel)
