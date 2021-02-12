package parse;


/**
 * Catch up the runtime exception indicating that the submitted query
 * has incorrect syntax.
 * @author linln
 */
@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException{
    public BadSyntaxException() {
        System.out.println("Bad Syntax Exception");
    }
}
