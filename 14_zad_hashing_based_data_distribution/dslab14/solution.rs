use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded, Included};

use crate::domain::{
    CHORD_FINGER_TABLE_MAX_ENTRIES, CHORD_RING_TABLE_MAX_ENTRIES, ChordAddr, ChordId, ChordLinkId,
    ChordMessage, ChordMessageHeader, ChordRoutingState, Internet, InternetMessage,
    chord_id_advance_by, chord_id_distance, chord_id_in_range, chord_id_max, chord_id_min,
};
use module_system::{Handler, ModuleRef};

/// A module representing a node in Chord.
pub(crate) struct ChordNode {
    /// The node's identifier on the ring.
    id: ChordId,
    /// The node's transport-layer address.
    addr: ChordAddr,
    /// The node's routing state.
    rs: ChordRoutingState,
    /// The interface to the Internet (no need to use directly).
    net_ref: ModuleRef<Internet>,
}

/// A Chord routing outcome.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ChordRoutingOutcome {
    /// Accepting a message by the routing node.
    Accept,
    /// Forwarding a message to the node with
    /// a given transport-layer address.
    Forward(ChordAddr),
}

impl ChordNode {
    pub(crate) fn new(
        net_ref: ModuleRef<Internet>,
        ring_bits: usize,
        // ring_redundancy is the parameter R from the learning section.
        ring_redundancy: usize,
        id: ChordId,
        addr: ChordAddr,
    ) -> Self {
        assert!(ring_bits >= 1);
        assert!(ring_bits <= CHORD_FINGER_TABLE_MAX_ENTRIES);
        assert!(ring_redundancy >= 1);
        assert!(ring_redundancy <= CHORD_RING_TABLE_MAX_ENTRIES);
        assert!(id <= chord_id_max(ring_bits));
        Self {
            id,
            addr,
            rs: ChordRoutingState {
                finger_table: vec![None; ring_bits],
                succ_table: vec![None; ring_redundancy],
                pred_table: vec![None; ring_redundancy],
            },
            net_ref,
        }
    }

    /// For each Chord node, creates a complete routing
    /// state given (an oracle's) information about all
    /// nodes in the system, that is, a mapping
    /// `ChordId` -> `ChordAddr`.
    #[allow(clippy::len_zero)]
    pub(crate) fn recreate_links_from_oracle(&mut self, all_nodes: &BTreeMap<ChordId, ChordAddr>) {
        assert!(
            self.rs.finger_table.len() > 0
                && self.rs.finger_table.len() <= CHORD_FINGER_TABLE_MAX_ENTRIES
        );
        assert!(
            self.rs.succ_table.len() > 0
                && self.rs.succ_table.len() <= CHORD_RING_TABLE_MAX_ENTRIES
        );
        assert!(self.rs.pred_table.len() == self.rs.succ_table.len());
        assert!(all_nodes.contains_key(&self.id));
        assert!(
            all_nodes
                .iter()
                .filter(|&(&k, &_v)| {
                    k < chord_id_min(self.rs.finger_table.len())
                        || k > chord_id_max(self.rs.finger_table.len())
                })
                .count()
                == 0
        );

        // FIXME: Implement this function. To this end, you may
        //        find the earlier chord_id_* functions useful.
        //        In essence, this function should only change
        //        self.rs, utilizing self.id and all_nodes.
        self.populate_succ_arr(all_nodes);
        self.populate_pred_arr(all_nodes);
        self.populate_finger_arr(all_nodes);
    }

    /// Given a header of a Chord message, decides
    /// what routing step the processing node should
    /// perform, that is, whether to accept the
    /// message or forward it to another node.
    pub(crate) fn find_next_routing_hop(
        &self, 
        hdr: &ChordMessageHeader
    ) -> ChordRoutingOutcome {
        // FIXME: Implement this function. To this end, you may
        //        find the earlier chord_id_* functions useful.
        //        In essence, this function requires only
        //        self.rs, self.id, and hdr.dst_id.
        let ring_bits = self.rs.finger_table.len();
        let dst_id = hdr.dst_id;

        // Firstly we check if we accept this msg, thus we take our first predecessor
        // and check if dst_id is in our range
        if let Some(pred) = self.rs.pred_table
            .first()
            .expect("find_next_routing_hop:: pred_table has 0 elements, there should be at least ONE element in it")
        {
            // Check if dst_id is in range (pred.id, self.id]
            // This means: dst_id > pred.id AND dst_id <= self.id (clockwise)
            if chord_id_in_range(
                ring_bits, 
                &dst_id, 
                (Excluded(pred.id), Included(self.id))) 
            {
                return ChordRoutingOutcome::Accept;
            }
        } 
        else
        {
            // No predecessor means this is the only node, so we accept this msg
            return ChordRoutingOutcome::Accept;
        }

        let forward = self.try_forward_to_pred_or_succ(&dst_id, ring_bits);

        if let Some(forward_addr) = forward
        {
            return ChordRoutingOutcome::Forward(forward_addr);
        }

        // We didn't find anyone to forward msg, thus we need to use finger table
        return self.forward_using_finger_table(&dst_id);
    }

    async fn recv_chord_msg(&mut self, mut msg: ChordMessage, _from_addr: &ChordAddr) {
        // Add self to the message as the next hop.
        msg.data.hops.push(self.id);
        // Route the message to self or another node.
        match self.find_next_routing_hop(&msg.hdr) {
            ChordRoutingOutcome::Accept => self.accept_chord_msg(msg).await,
            ChordRoutingOutcome::Forward(addr) => self.send_chord_msg(msg, addr).await,
        }
    }

    async fn send_chord_msg(&self, msg: ChordMessage, to_addr: ChordAddr) {
        let net_msg = InternetMessage {
            src: self.addr,
            dst: to_addr,
            body: msg,
        };
        self.net_ref.send(net_msg).await;
    }

    #[allow(clippy::unused_async)]
    async fn accept_chord_msg(&self, msg: ChordMessage) {
        msg.data.delivery_notifier.send(msg.data.hops).unwrap();
    }

    #[cfg(test)]
    pub(crate) fn fetch_routing_state(&self) -> ChordRoutingState {
        self.rs.clone()
    }

    #[cfg(test)]
    pub(crate) fn replace_routing_state(&mut self, rs: ChordRoutingState) {
        self.rs = rs;
    }

    fn populate_pred_arr(
        &mut self,
        all_nodes: &BTreeMap<ChordId, ChordAddr>
    )
    {
        let mut pred_idx: usize = 0;

        for (chord_id, chord_addr) in all_nodes.range(..self.id).rev()
        {
            let val = self.rs.pred_table.get_mut(pred_idx).unwrap(); 
            *val = Some(ChordLinkId {
                id: *chord_id,
                addr: *chord_addr 
            });
            pred_idx += 1;

            if pred_idx >= self.rs.pred_table.len()
            {
                break;
            }
        }

        if pred_idx < self.rs.pred_table.len()
        {
            for (chord_id, chord_addr) in all_nodes.iter().rev()
            {
                if pred_idx >= self.rs.pred_table.len() || self.id == *chord_id
                {
                    break;
                }

                let val = self.rs.pred_table.get_mut(pred_idx).unwrap(); 
                *val = Some(ChordLinkId {
                    id: *chord_id,
                    addr: *chord_addr 
                });
                pred_idx += 1;
            }
        }
    }

    fn populate_succ_arr(
        &mut self,
        all_nodes: &BTreeMap<ChordId, ChordAddr>
    )
    {
        let mut succ_idx: usize = 0;

        // we take all ids to the right to our id, without our id
        for (chord_id, chord_addr) in all_nodes.range((Excluded(self.id), Unbounded))
        {
            let val = self.rs.succ_table.get_mut(succ_idx).unwrap(); 
            *val = Some(ChordLinkId {
                id: *chord_id,
                addr: *chord_addr 
            });
            succ_idx += 1;

            if succ_idx >= self.rs.succ_table.len()
            {
                break;
            }
        }

        // If there is still place in succ_table, we start adding new ids from the 
        // beginning, but we firstly check if current id IS NOT OUR ID 
        // (i.e. we are first id and we would add the same ids again)
        if succ_idx < self.rs.succ_table.len()
        {
            for (chord_id, chord_addr) in all_nodes.iter()
            {
                if succ_idx >= self.rs.succ_table.len() || self.id == *chord_id
                {
                    break;
                }

                let val = self.rs.succ_table.get_mut(succ_idx).unwrap(); 
                *val = Some(ChordLinkId {
                    id: *chord_id,
                    addr: *chord_addr 
                });
                succ_idx += 1;
            }
        }
    }

    fn populate_finger_arr(
        &mut self,
        all_nodes: &BTreeMap<ChordId, ChordAddr>
    )
    {
        let ring_bits = self.rs.finger_table.len();

        for i in 0..ring_bits
        {
            let delta_left = 1u128 << i;
            let left_bound = chord_id_advance_by(
                ring_bits, 
                &self.id, 
                &delta_left
            ); 
            let right_bound = if i + 1 < ring_bits {
                let delta_right = 1u128 << (i + 1);
                chord_id_advance_by(
                ring_bits, 
                &self.id, 
                &delta_right
            ) 
            } else
            {
                // self.id since x + m (mod m) = x, in our case i + 1 == ring_bits
                self.id
            };
            let mut found: Option<ChordLinkId> = None;

            // normal case, no overflow, we check [left_bound, right_bound) range
            if left_bound < right_bound
            {
                for (chord_id, chord_addr) in all_nodes.range(left_bound..)
                {
                    if chord_id_in_range(ring_bits, chord_id, left_bound..right_bound)
                    {
                        found = Some(ChordLinkId {
                            id: *chord_id,
                            addr: *chord_addr,
                        });
                        break;
                    }
    
                    if *chord_id >= right_bound
                    {
                        break;
                    }
                }
                *self.rs.finger_table.get_mut(i).unwrap() = found;
            }
            else // left_bound > right_bound
            {
                // We've got overflow, thus we check from left_bound to max
                // and then we need to check from beginning till right_bound
                for (chord_id, chord_addr) in all_nodes.range(left_bound..)
                {
                    if chord_id_in_range(ring_bits, chord_id, left_bound..=chord_id_max(ring_bits))
                    {
                        found = Some(ChordLinkId {
                            id: *chord_id,
                            addr: *chord_addr,
                        });
                        break;
                    }
                }

                // If we found something, no need to check 0..right_bound range
                if found.is_some()
                {
                    *self.rs.finger_table.get_mut(i).unwrap() = found;
                }
                else
                {
                    for (chord_id, chord_addr) in all_nodes.range(..right_bound)
                    {
                        if chord_id_in_range(
                            ring_bits, 
                            chord_id, 
                            chord_id_min(ring_bits)..right_bound
                        )
                        {
                            found = Some(ChordLinkId {
                                id: *chord_id,
                                addr: *chord_addr,
                            });
                            break;
                        }

                        if *chord_id >= right_bound
                        {
                            break;
                        }
                    }
                    *self.rs.finger_table.get_mut(i).unwrap() = found;
                }
            }
        }
    }

    fn forward_using_finger_table(
        &self,
        dst_id: &ChordId,
    ) -> ChordRoutingOutcome
    {
        let ring_bits = self.rs.finger_table.len();

        // We iterate from end to start, since highest indexes are farther from us on
        // the ring and we want to find the highest one
        for finger_opt in self.rs.finger_table.iter().rev() {
            if let Some(finger_link) = finger_opt {
                if chord_id_in_range(
                    ring_bits,
                    &finger_link.id,
                    (Excluded(self.id), Included(*dst_id)),
                ) {
                    return ChordRoutingOutcome::Forward(finger_link.addr);
                }
            }
        }

        // This should never happen since we firstly check if there is only one 
        // node, and if there are more nodes, there must be nodes in finger table
        // thus we must find node we want to forward to.
        panic!("forward_using_finger_table:: didnt find finger_link.addr we need to forward to, this should never happen");
    }

    fn try_forward_to_pred_or_succ(
    &self,
    dst_id: &ChordId,
    ring_bits: usize
    ) -> Option<ChordAddr> 
    {
        // Firstly we simply check if we have such id in our tables
        if let Some(Some(succ)) = self.rs.succ_table.first() {
            if succ.id == *dst_id {
                return Some(succ.addr);
            }
        }
        if let Some(Some(pred)) = self.rs.pred_table.first() {
            if pred.id == *dst_id {
                return Some(pred.addr);
            }
        }

        // If not we check ranges

        // For our first successor we are his predecessor, thus we start with pred
        // equal to our id
        let mut pred = self.id;

        for succ in &self.rs.succ_table 
        {
            if let Some(succ_link) = succ 
            {
                // If successor is not None, we check if dst_id is 
                // in range (pred, succ]
                if chord_id_in_range(
                    ring_bits,
                    dst_id,
                    (Excluded(pred), Included(succ_link.id))
                ) 
                {
                    // If it is we need to forward it to succ_link.addr
                    return Some(succ_link.addr);
                }
                else
                {
                    pred = succ_link.id;
                }
            }
        }

        let mut curr = None;

        for pred in self.rs.pred_table.iter()
        {
            if let Some(pred) = pred
            {
                // At first curr is None, so we just save first value from pred_table
                if curr.is_none()
                {
                    curr = Some(pred.clone());
                }
                else
                {
                    // afterwards curr will never be None, so now we check range
                    // (pred, curr], and if id is in this range we return curr.addr
                    if let Some(curr_link) = curr
                    {
                        if chord_id_in_range(
                            ring_bits,
                            dst_id,
                            (Excluded(pred.id), Included(curr_link.id))
                        ) {
                            return Some(curr_link.addr);
                        }
                        else
                        {
                            curr = Some(pred.clone());
                        }
                    }
                }

            }
        }

        return None;
    }
}

#[async_trait::async_trait]
impl Handler<InternetMessage> for ChordNode {
    async fn handle(&mut self, msg: InternetMessage) {
        assert_eq!(msg.dst, self.addr);
        self.recv_chord_msg(msg.body, &msg.src).await;
    }
}
