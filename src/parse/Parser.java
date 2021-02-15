package parse;

import java.util.*;

import query.Constant;
import query.Expression;
import query.Predicate;
import query.Term;
//import query.*;

/**
 * Parser of the CosmicDB
 * @author linln
 */
public class Parser {
    private Lexer lex;

    public Parser(String s){
        lex = new Lexer(s);
    }

    public String getField() { return lex.eatIdf();}
    public Constant getConstant() { return lex.matchString() ? new Constant(lex.eatStringConstant()) : new Constant(lex.eatIntConstant()) ;}
    public Expression getExpression() {return lex.matchIdf() ? new Expression(getField()) : new Expression(getConstant()) ;}
    public Term getTerm() {
        Expression lhs = getExpression();
        lex.eatDelim('=');
        Expression rhs = getExpression();
        return new Term(lhs, rhs);
    }
    public Predicate predicate() {
        Predicate pred = new Predicate(getTerm());
        if (lex.matchKeyWord("and")) {
            lex.eatKeyWord("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    /**
     * 解析简单的sql命令
     * @return
     */

    public QueryData select() {
        lex.eatKeyWord("select");
        List<String> fields = selectList();
        lex.eatKeyWord("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();
        if (lex.matchKeyWord("where")) {
            lex.eatKeyWord("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }

    private List<String> selectList() {
        List<String> L = new ArrayList<String>();
        L.add(getField());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(selectList());
        }
        return L;
    }

    private Collection<String> tableList() {
        Collection<String> L = new ArrayList<String>();
        L.add(lex.eatIdf());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(tableList());
        }
        return L;
    }

// Methods for parsing the various update commands

    public Object updateCmd() {
        if (lex.matchKeyWord("insert")) {
            return insert();
        } else if (lex.matchKeyWord("delete")) {
            return delete();
        } else if (lex.matchKeyWord("update")) {
            return modify();
        } else {
            return create();
        }
    }

    private Object create() {
        lex.eatKeyWord("create");
        if (lex.matchKeyWord("table")) {
            return createTable();
        } else if (lex.matchKeyWord("view")) {
            return createView();
        } else {
            return createIndex();
        }
    }

// Method for parsing delete commands

    public DeleteData delete() {
        lex.eatKeyWord("delete");
        lex.eatKeyWord("from");
        String tblname = lex.eatIdf();
        Predicate pred = new Predicate();
        if (lex.matchKeyWord("where")) {
            lex.eatKeyWord("where");
            pred = predicate();
        }
        return new DeleteData(tblname, pred);
    }

// Methods for parsing insert commands

    public InsertData insert() {
        lex.eatKeyWord("insert");
        lex.eatKeyWord("into");
        String tblname = lex.eatIdf();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyWord("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }

    private List<String> fieldList() {
        List<String> L = new ArrayList<String>();
        L.add(getField());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<Constant>();
        L.add(getConstant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }

// Method for parsing modify commands

    public ModifyData modify() {
        lex.eatKeyWord("update");
        String tblname = lex.eatIdf();
        lex.eatKeyWord("set");
        String fldname = getField();
        lex.eatDelim('=');
        Expression newval = getExpression();
        Predicate pred = new Predicate();
        if (lex.matchKeyWord("where")) {
            lex.eatKeyWord("where");
            pred = predicate();
        }
        return new ModifyData(tblname, fldname, newval, pred);
    }

// Method for parsing create table commands

    public CreateTableData createTable() {
        lex.eatKeyWord("table");
        String tblname = lex.eatIdf();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldname = getField();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lex.matchKeyWord("int")) {
            lex.eatKeyWord("int");
            schema.addIntField(fldname);
        }
        else {
            lex.eatKeyWord("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }

// Method for parsing create view commands

    public CreateViewData createView() {
        lex.eatKeyWord("view");
        String viewname = lex.eatIdf();
        lex.eatKeyWord("as");
        QueryData qd = query();
        return new CreateViewData(viewname, qd);
    }


//  Method for parsing create simpledb.index commands

    public CreateIndexData createIndex() {
        lex.eatKeyWord("simpledb/index");
        String idxname = lex.eatIdf();
        lex.eatKeyWord("on");
        String tblname = lex.eatIdf();
        lex.eatDelim('(');
        String fldname = getField();
        lex.eatDelim(')');
        return new CreateIndexData(idxname, tblname, fldname);
    }


}
