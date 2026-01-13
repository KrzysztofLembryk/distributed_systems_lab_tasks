use std::collections::{HashMap, HashSet, VecDeque};
use crate::domain::{Action, ClientRef, Edit, EditRequest, Operation, ReliableBroadcastRef};
use module_system::Handler;

type RoundT = u64;
type ProcRankT = usize;

impl Operation {
    pub fn transform_operation(
        op_to_transform: &mut Operation,
        starting_index: usize,
        log: &Vec<Operation>
    )
    {
        for op in log[starting_index..].iter()
        {
            op_to_transform.transform(op);
        }
    }

    fn transform(&mut self, op: &Operation)
    {
        match op.action
        {
            Action::Insert { .. } => {
                self.transform_wrt_insert(op);
            },
            Action::Delete { .. } => {
                self.transform_wrt_delete(op);
            },
            Action::Nop => {
                // we do no transformation for Nop
            }
        }
    }

    // Add any methods you need.
    fn transform_wrt_insert(&mut self, insert_op: &Operation)
    {
        match insert_op.action
        {
            Action::Insert { idx: insert_idx, .. } => {
                match &self.action
                {
                    Action::Insert { idx: self_idx, ch } => {
                        if *self_idx < insert_idx
                        {
                            // we don't change anything
                        }
                        else if *self_idx == insert_idx 
                            && self.process_rank < insert_op.process_rank
                        {
                            // we don't change anything
                        }
                        else
                        {
                            self.action = Action::Insert { 
                                idx: *self_idx + 1, 
                                ch: *ch 
                            };
                        }
                    },
                    Action::Delete { idx: self_idx } => {
                        if *self_idx < insert_idx
                        {
                            // we don't change anything
                        }
                        else
                        {
                            self.action = Action::Delete { idx: *self_idx + 1 };
                        }
                    },
                    Action::Nop => {
                        // we do nothing here
                    }
                }
            },
            _ => {
                panic!("Operation::transform_wrt_insert:: got not an INSERT action");
            }
        }
    }

    fn transform_wrt_delete(&mut self, delete_op: &Operation)
    {
        match delete_op.action
        {
            Action::Delete { idx: delete_idx, .. } => {
                match &self.action
                {
                    Action::Insert { idx: self_idx, ch } => {
                        if *self_idx <= delete_idx
                        {
                            // nothing changes
                        }
                        else
                        {
                            self.action = Action::Insert { 
                                idx: *self_idx - 1, 
                                ch:  *ch
                            };
                        }
                    },
                    Action::Delete { idx: self_idx } => {
                        if *self_idx < delete_idx
                        {
                            // nothing changes
                        }
                        else if *self_idx == delete_idx
                        {
                            self.action = Action::Nop;
                        }
                        else
                        {
                            self.action = Action::Delete { idx: *self_idx - 1 };
                        }
                    },
                    Action::Nop => {
                        // we do nothing here
                    }
                }
            },
            _ => {
                panic!("Operation::transform_wrt_delete:: got not a DELETE action");
            }
        }
    }
}

/// Process of the system.
pub(crate) struct Process<const N: usize> 
{
    /// Rank of the process.
    rank: ProcRankT,
    /// Reference to the broadcast module.
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    /// Reference to the process's client.
    client: Box<dyn ClientRef>,
    // ################### Add any fields you need. ###################
    n_proc: usize,
    // Ranks of procs that in our current round we got msg from, once we get msg from
    // all procs we can go to the next round
    received_from: HashSet<ProcRankT>,
    curr_round: RoundT,
    curr_round_first_log_idx: usize,
    log: Vec<Operation>,
    // Operations within a round are always processed in the order they are received.
    // If in our current round we got op for different round we store it
    // How to check if op is for given round:
    // 1) first we check received_from - if proc is not there it means its op is for
    //  our curr rank
    // 2) if it is there we check curr_round + 1 in ops_for_future_rounds and search
    //  for an entry with given proc rank till we find round that doesn't have 
    //  this process' rank and we append new operation
    ops_for_future_rounds: HashMap<RoundT, Vec<Operation>>, 
    requests_from_client: VecDeque<EditRequest>,
}

impl<const N: usize> Process<N> {
    pub(crate) fn new(
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> Self {
        return Self {
            rank,
            broadcast,
            client,
            // Add any fields you need.
            n_proc: N,
            received_from: HashSet::new(),
            curr_round: 0,
            curr_round_first_log_idx: 0,
            log: Vec::new(),
            ops_for_future_rounds: HashMap::new(),
            requests_from_client: VecDeque::new(),
        };
    }

    fn reinitialize(&mut self)
    {
        self.received_from.clear();
        self.ops_for_future_rounds.remove(&self.curr_round);
    }

    // Add any methods you need.
    pub fn insert_op_for_future_round(&mut self, new_op: Operation)
    {
        let mut next_round = self.curr_round + 1;

        loop 
        {
            if let Some(next_round_ops) = 
                self.ops_for_future_rounds.get_mut(&next_round)
            {
                let mut op_found: bool = false;

                for op in next_round_ops.iter()
                {
                    if op.process_rank == new_op.process_rank
                    {
                        op_found = true;
                        break;
                    }
                }
                // If we haven't found operation with new_op.process_rank this means
                // that this operation is for current next_round so we add it
                if !op_found
                {
                    next_round_ops.push(new_op);
                    return;
                }
            }
            else
            {
                // If there is no entry for next_round this means that new_op is 
                // first operation to append for this round.
                self.ops_for_future_rounds.insert(next_round, vec![new_op]);
                return;
            }

            next_round += 1;
        }
    }

    pub async fn apply_client_op(&mut self, client_op: &EditRequest) -> Operation
    {
        let op_num_applied = client_op.num_applied;
        let mut client_op = Operation { 
            process_rank: self.rank,
            action: client_op.action.clone()
        }; 

        // We set temporary rank before transform
        client_op.process_rank = self.n_proc; 
        Operation::transform_operation(
            &mut client_op,
            op_num_applied,
            &self.log
        );

        // We return to our rank
        client_op.process_rank = self.rank;

        return client_op;
    }

    pub async fn start_new_round_when_full(&mut self) 
    {
        loop {
            let mut do_next_round: bool = false;

            self.reinitialize();
    
            if !self.requests_from_client.is_empty()
            {
                self.start_new_round_with_client_op().await;
                do_next_round = self.apply_pending_ops_for_curr_round().await;
            }
            else if !self.ops_for_future_rounds.is_empty()
            {
                // There is no client msg thus our first msg is nop, and then we handle
                // stored msgs
                self.start_new_round_with_nop().await;            
                do_next_round = self.apply_pending_ops_for_curr_round().await;
            }
            // else both of them are empty so we return

            if !do_next_round
            {
                break;
            }
        }
    }

    pub async fn start_new_round_with_client_op(&mut self)
    {
        self.curr_round += 1;

        self.received_from.insert(self.rank);

        // We prioritize client requests, if there is one and we start new
        // round, we immediately add it as our first operation in this round
        let client_request = self.requests_from_client.pop_front().unwrap();
        let client_op = self.apply_client_op(&client_request).await;

        self.client.send(Edit { action: client_op.action.clone() }).await;
        self.broadcast.send(client_op.clone()).await;
        self.log.push(client_op);

        self.curr_round_first_log_idx = self.log.len() - 1;
    }

    pub async fn start_new_round_with_nop(&mut self)
    {
        self.curr_round += 1;

        self.received_from.insert(self.rank);
        
        let no_op = Operation {
            process_rank: self.rank,
            action: Action::Nop
        };

        self.client.send(Edit { action: Action::Nop }).await;
        self.broadcast.send(no_op.clone()).await;
        self.log.push(no_op);

        self.curr_round_first_log_idx = self.log.len() - 1;
    }

    async fn apply_pending_ops_for_curr_round(&mut self) -> bool
    {
        if let Some(pending_ops) = self.ops_for_future_rounds
                                    .get_mut(&self.curr_round)
        {
            for op in pending_ops.iter_mut()
            {
                // We must insert process rank from pending op since we don't want
                // to get second op for this round from the same process
                assert!(
                    !self.received_from.contains(&op.process_rank), 
                    "apply_pending_ops_for_curr_round:: received_from already contains proc_rank: '{}'", op.process_rank
                );
                self.received_from.insert(op.process_rank);

                Operation::transform_operation(
                    op, 
                    self.curr_round_first_log_idx,
                    &self.log
                );

                self.client.send(Edit { action: op.action.clone() }).await;
                self.log.push(op.clone());
            }
            // After applying all stored ops we remove them from our map, since 
            // either we applied all ops for this round OR we need to wait for them
            // but if we need to wait for them, we will apply them as they come
            self.ops_for_future_rounds.remove(&self.curr_round);

            if self.received_from.len() == self.n_proc
            {
                // If all ops for given round were applied we start next round if
                // possible
                return true;
            }
        }
        // If there are no pending ops we return false to signalise there is no 
        // another round
        return false;
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> 
{
    async fn handle(&mut self, msg: Operation) 
    {
        if self.received_from.is_empty()
        {
            self.start_new_round_with_nop().await;
        }

        if !self.received_from.contains(&msg.process_rank)
        {
            // This means that in current round we haven't got msg from this proc yet
            // so we insert this proc 
            self.received_from.insert(msg.process_rank);

            let mut op = msg;
            // To handle msg, firstly we transform it by applying transformations 
            // based on all previous operations in order in current round
            Operation::transform_operation(
                &mut op, 
                self.curr_round_first_log_idx,
                &self.log
            );
            // then send what action our client needs to do
            self.client.send(Edit {action: op.action.clone()}).await;
            // and finally append it to our log
            self.log.push(op);

            if self.received_from.len() == self.n_proc
            {
                // this means that this operation was last in current round, thus 
                // we need to start new round
                self.start_new_round_when_full().await;
            }
        }
        else
        {
            // In current round we've already got operation from this proc, so we 
            // need to add it to operations for future rounds
            self.insert_op_for_future_round(msg);
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> 
{
    async fn handle(&mut self, request: EditRequest) 
    {
        self.requests_from_client.push_back(request);

        if self.received_from.is_empty()
        {
            self.start_new_round_with_client_op().await;
        }
    }
}
