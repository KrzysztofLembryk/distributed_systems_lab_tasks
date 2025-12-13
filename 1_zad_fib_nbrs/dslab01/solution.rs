pub struct Fibonacci {
    // Add here any fields you need.
    prev_nbr: u128,
    curr_nbr: u128,
    idx: usize,
    is_overflow: bool,
}

impl Fibonacci {
    /// Create new `Fibonacci`.
    pub fn new() -> Fibonacci {
        Fibonacci { 
            prev_nbr: 0,
            curr_nbr: 1,
            idx: 0,
            is_overflow: false,
        }
    }

    /// Calculate the n-th Fibonacci number.
    ///
    /// This shall not change the state of the iterator.
    /// The calculations shall wrap around at the boundary of u8.
    /// The calculations might be slow (recursive calculations are acceptable).
    pub fn fibonacci(n: usize) -> u8 {
        if n == 0
        {
            return 0;
        }
        else if n == 1
        {
            return 1;
        }
        else 
        {
            return Fibonacci::fibonacci(n - 1).wrapping_add(Fibonacci::fibonacci(n - 2));
        }
    }
}

impl Iterator for Fibonacci {
    type Item = u128;

    /// Calculate the next Fibonacci number.
    ///
    /// The first call to `next()` shall return the 0th Fibonacci number (i.e., `0`).
    /// The calculations shall not overflow and shall not wrap around. If the result
    /// doesn't fit u128, the sequence shall end (the iterator shall return `None`).
    /// The calculations shall be fast (recursive calculations are **un**acceptable).
    fn next(&mut self) -> Option<Self::Item> 
    {
        if self.is_overflow
        {
            return None;
        }

        if self.idx == 0
        {
            self.idx += 1;
            return Some(0);
        }

        if self.idx == 1
        {
            self.idx += 1;
            return Some(1);
        }

        match self.prev_nbr.checked_add(self.curr_nbr)
        {
            Some(new_val) => {
                self.prev_nbr = self.curr_nbr;
                self.curr_nbr = new_val;
                self.idx += 1;
                return Some(new_val);
            }
            None => {
                self.is_overflow = true;
                return None;
            }
        }
        
    }
}
