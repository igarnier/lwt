(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2021 Nomadic Labs <contact@nomadic-labs.com>                *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

module Elt = struct
  (* we use an increasing unique id to ensure a
     total ordering for promises waking up at the same time
     consistent with the order of insertion of the queue. *)
  type t = {resolver : unit Lwt.u; wakeup_at : float; iuid : int}

  let gen =
    let x = ref 0 in
    fun () ->
      let v = !x in
      incr x ; v

  let compare x y =
    let c = Float.compare x.wakeup_at y.wakeup_at in
    if c = 0 then Int.compare x.iuid y.iuid else c
end

module Prio = Float
module Prioqueue = Binary_heap.Make (Elt)

module Create (X : sig
  val initial_time : float
end) =
struct
  type t = {mutable now : float; mutable queue : Prioqueue.t}

  let now = ref X.initial_time

  let dummy =
    let (_, never_used) = Lwt.task () in
    Elt.{resolver = never_used; wakeup_at = 0.0; iuid = -1}

  let queue = Prioqueue.create ~dummy 10

  let rec scheduler queue =
    match Prioqueue.minimum queue with
    | exception Binary_heap.Empty ->
        ()
    | Elt.{resolver; wakeup_at; _} ->
        if wakeup_at <= !now then (
          Prioqueue.remove queue ;
          Lwt.wakeup_later resolver () ;
          scheduler queue )
        else ()

  let set_now seconds_since_epoch =
    assert (seconds_since_epoch > 0.) ;
    now := seconds_since_epoch ;
    scheduler queue

  let sleep seconds =
    let (promise, resolver) = Lwt.task () in
    let iuid = Elt.gen () in
    let wakeup_at = !now +. seconds in
    let elt = Elt.{resolver; wakeup_at; iuid} in
    Prioqueue.add queue elt ; promise

  let now () = !now
end
