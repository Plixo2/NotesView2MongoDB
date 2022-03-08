import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ExportConfig {
	private String viewName;
	private Set<String> fields;
	private String collection;

	public ExportConfig(String viewName, String collection, String... fields) {
		this.viewName = viewName;
		this.fields = new HashSet<>(Arrays.asList(fields));
		this.collection = collection;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

}
