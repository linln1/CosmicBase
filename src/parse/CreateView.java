package parse;

public class CreateView {

    private String viewname;
    private QueryData qrydata;

    /**
     * Saves the view name and its definition.
     */
    public CreateView(String viewname, QueryData qrydata) {
        this.viewname = viewname;
        this.qrydata = qrydata;
    }

    public String getViewname() {
        return viewname;
    }

    public String getViewDef() {
        return qrydata.toString();
    }
}
