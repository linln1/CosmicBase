package parse;

import java.util.*;
import java.io.*;

/**
 * Lexer
 * @author linln
 */
public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer token;

    /**
     * 不区分大小写，标识符不允许.
     * @param s
     */
    public Lexer(String s){
        initKW();
        token = new StreamTokenizer(new StringReader(s));
        token.ordinaryChar('.'); // "."标记为非法
        token.wordChars('_', '_');
        token.lowerCaseMode(true);
        nextToken();
    }

    /**
     * 将token流按空格隔开，一个一个处理
     */
    private void nextToken(){
        try{
            token.nextToken();
        }
        catch (IOException e){
            throw new BadSyntaxException();
        }
    }

    private void initKW(){
        keywords = Arrays.asList("create", "drop", "select", "from", "where",
                "insert", "into", "values", "delete", "update", "set", "as", "table", "primary",
                "int", "real", "varchar", "view", "as", "index", "on", "using", "group", "by",
                "desc", "ordered", "asc", "not", "null", "use");
    }

    /**
     * 判定该token是哪一种类型的符号
     * 界符， 运算符，常量， 定义符， 标识符
     */
    public boolean matchDelim(char d) {
        return d == (char)token.ttype;
    }
    public boolean matchIntConstant() { return token.ttype == StreamTokenizer.TT_NUMBER;}
    public boolean matchString() {
        return '\'' == (char)token.ttype;
    }
    public boolean matchKeyWord(String kw) {return token.ttype == StreamTokenizer.TT_WORD && token.sval.equals(kw); }
    public boolean matchIdf() {
        return  token.ttype==StreamTokenizer.TT_WORD && !keywords.contains(token.sval);
    }

    /**
     * scan the token stream to match
     * @param d
     */
    public void eatDelim(char d) {
        if (!matchDelim(d)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }
    public int eatIntConstant() {
        if (!matchIntConstant()) {
            throw new BadSyntaxException();
        }
        int i = (int) token.nval;
        nextToken();
        return i;
    }
    public String eatStringConstant() {
        if (!matchString()) {
            throw new BadSyntaxException();
        }
        String s = token.sval; //constants are not converted to lower case
        nextToken();
        return s;
    }
    public void eatKeyWord(String kw) {
        if (!matchKeyWord(kw)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }
    public String eatIdf() {
        if (!matchIdf()) {
            throw new BadSyntaxException();
        }
        String s = token.sval;
        nextToken();
        return s;
    }
}
