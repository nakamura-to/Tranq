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

/// Used with parsing and lexing.
module Text =

  /// Represents a location in a text
  type Location = 
    { pos_fname : string; 
      pos_lnum : int; 
      pos_bol : int; 
      pos_cnum : int; }