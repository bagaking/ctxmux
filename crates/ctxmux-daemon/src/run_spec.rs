use ctxmux_protocol::{RunSpec, TerminalSize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum RunSpecValidationError {
    #[error("Run program must not be empty")]
    EmptyProgram,
    #[error("Run input references must not be empty")]
    EmptyInputReference,
    #[error("terminal rows and columns must be greater than zero")]
    ZeroTerminalDimension,
}

pub(crate) fn validate_run_spec(spec: &RunSpec) -> Result<(), RunSpecValidationError> {
    if spec.program.is_empty() {
        return Err(RunSpecValidationError::EmptyProgram);
    }
    if spec
        .declared_inputs
        .iter()
        .any(|input| input.reference.is_empty())
    {
        return Err(RunSpecValidationError::EmptyInputReference);
    }
    validate_terminal_size(spec.size)
}

pub(crate) const fn validate_terminal_size(
    size: TerminalSize,
) -> Result<(), RunSpecValidationError> {
    if size.cols == 0 || size.rows == 0 {
        return Err(RunSpecValidationError::ZeroTerminalDimension);
    }
    Ok(())
}
