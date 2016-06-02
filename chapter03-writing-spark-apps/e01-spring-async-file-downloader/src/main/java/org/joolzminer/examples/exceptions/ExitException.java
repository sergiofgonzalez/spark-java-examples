package org.joolzminer.examples.exceptions;

import org.springframework.boot.ExitCodeGenerator;

@SuppressWarnings("serial")
public class ExitException extends RuntimeException implements ExitCodeGenerator {

    public ExitException(Throwable cause) {
        super(cause);
    }
	
	@Override
	public int getExitCode() {
		return 10;
	}
}
