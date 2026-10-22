use super::{Command, Queue, State};

trait Erased: Send {
    fn apply(self: Box<Self>, state: &mut State);
}

impl<C: Command> Erased for C {
    fn apply(self: Box<Self>, state: &mut State) {
        (*self).apply(state);
    }
}

#[derive(Default)]
pub struct Boxed {
    commands: Vec<Box<dyn Erased>>,
}

impl Queue for Boxed {
    fn push<C: Command>(&mut self, command: C) {
        self.commands.push(Box::new(command));
    }

    fn apply(&mut self, state: &mut State) {
        for command in self.commands.drain(..) {
            command.apply(state);
        }
    }

    fn discard(&mut self) {
        self.commands.clear();
    }
}
