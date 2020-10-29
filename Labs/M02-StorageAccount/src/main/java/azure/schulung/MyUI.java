package azure.schulung;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobSignedIdentifier;
import com.azure.storage.blob.models.PublicAccessType;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasQueryParameters;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.event.selection.SelectionEvent;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.ui.*;

import javax.servlet.annotation.WebServlet;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * To start this server application use 'mvn clean install jetty:run'
 * 
 */
@Theme("mytheme")
public class MyUI extends UI {

	final static String CONNECTION_KEY = "DefaultEndpointsProtocol=https;AccountName=uebung2;AccountKey=Ejv3MUBx/8GQLTbNEpGI4gVsFNkl6DrSa4Fr7objmpmbhnACKpD6pZIXMVbGt+XlN7hSHCLp+v40+k/trIz+2Q==;EndpointSuffix=core.windows.net";

	final static String STORAGE_ACCOUNT_NAME = "uebung2";

	final static String STORAGE_ACCOUNT_KEY = "Ejv3MUBx/8GQLTbNEpGI4gVsFNkl6DrSa4Fr7objmpmbhnACKpD6pZIXMVbGt+XlN7hSHCLp+v40+k/trIz+2Q==";

	TreeGrid<Entry> treeGrid = new TreeGrid<>();

	Grid<BlobSignedIdentifier> bsi = new Grid<>("Access");

	TextField url = new TextField("URL");

	TextArea metaData = new TextArea("Metadata");
	private BlobServiceClient serviceClient;

	@Override
	protected void init(VaadinRequest vaadinRequest) {

		HttpClient client = new NettyAsyncHttpClientBuilder().build();
		serviceClient = new BlobServiceClientBuilder().httpClient(client).connectionString(CONNECTION_KEY)
				.buildClient();

		final VerticalLayout layout = new VerticalLayout();

		treeGrid.addColumn(Entry::getName).setCaption("Name");
		treeGrid.addColumn(Entry::getAccessLevel).setCaption("AccessLevel");
		treeGrid.addColumn(e -> e.accessPolicies).setCaption("Policies");
		treeGrid.addColumn(e -> e.lastModified).setCaption("Modified");
		treeGrid.addColumn(e -> e.accessTier).setCaption("Access Tier");
		treeGrid.addColumn(e -> e.blobTier).setCaption("Blob Tier");
		treeGrid.setWidth("100%");
		treeGrid.addSelectionListener(e -> select(e));

		bsi.addColumn(e -> e.getId()).setCaption("Id");

		metaData.setWidth("100%");
		bsi.setWidth("100%");

		Button button = new Button("Update");
		button.addClickListener(e -> this.update());

		HorizontalLayout hl = new HorizontalLayout();

		Button createSASB = new Button("createSAS", e -> createSAS());

		hl.addComponents(button, new Button("Create Lease", e -> createLease()), createSASB);

		url.setWidth("100%");
		layout.addComponents(hl, url, treeGrid, metaData, bsi);

		update();

		setContent(layout);
	}

	private void createLease() {
		Set<Entry> set = treeGrid.getSelectedItems();
		if (!set.isEmpty()) {
			Entry entry = set.iterator().next();

			if (entry.blobcc != null) {
				BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().containerClient(entry.blobcc)
						.buildClient();
				blobLeaseClient.acquireLease(60);
				Notification.show("Lease Setted!");
			} else if (entry.blobClient != null) {
				BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(entry.blobClient)
						.buildClient();
				blobLeaseClient.acquireLease(60);
				Notification.show("Lease Setted!");
			} else {
				Notification.show("No blobcc");
			}
		} else {
			Notification.show("No Item");
		}
	}

	private void createSAS() {

		Set<Entry> set = treeGrid.getSelectedItems();
		if (set.isEmpty()) {
			Notification.show("No selction");
			return;
		}
		Entry entry = set.iterator().next();
		if (entry.blobClient == null) {
			Notification.show("No File selected.");
			return;
		}

		String cname = entry.blobClient.getContainerName();
		String fname = entry.blobClient.getBlobName();

		BlobSasPermission blobPermission = new BlobSasPermission().setReadPermission(true);

		BlobServiceSasSignatureValues builder = new BlobServiceSasSignatureValues().setProtocol(SasProtocol.HTTPS_ONLY)
				.setExpiryTime(OffsetDateTime.now().plusDays(2)).setContainerName(cname).setBlobName(fname);

		if (!bsi.getSelectedItems().isEmpty()) {

			BlobSignedIdentifier identifier = bsi.getSelectedItems().iterator().next();
			System.out.println("perm=" + identifier.getId());
			builder = builder.setIdentifier(identifier.getId());
		} else {
			builder = builder.setPermissions(blobPermission);
		}

		System.out.println("" + entry.blobClient.getBlobUrl());

		StorageSharedKeyCredential credential = new StorageSharedKeyCredential(STORAGE_ACCOUNT_NAME,
				STORAGE_ACCOUNT_KEY);
		BlobServiceSasQueryParameters sasQueryParameters = builder.generateSasQueryParameters(credential);
		System.out.println(entry.blobClient.getBlobUrl() + "?" + sasQueryParameters.encode());

		url.setValue(entry.blobClient.getBlobUrl() + "?" + sasQueryParameters.encode());
	}

	private Object select(SelectionEvent<Entry> e) {
		if (e.getFirstSelectedItem().isPresent()) {
			Entry entry = e.getFirstSelectedItem().get();
			if (entry.metadata != null) {
				String value = "";
				for (String key : entry.metadata.keySet()) {
					value += key + "=" + entry.metadata.get(key) + "\n";
				}
				metaData.setValue(value);
			} else {
				metaData.setValue("-" + e.getFirstSelectedItem().get().name);
			}
			if (entry.accessPolicies != null) {
				bsi.setItems(entry.accessPolicies);
			}
		}
		return null;
	}

	private void update() {
		Entry root = new Entry();
		root.setName("Root");
		PagedIterable<BlobContainerItem> listContainer = serviceClient.listBlobContainers();
		for (BlobContainerItem container : listContainer) {
			Entry containerEntry = new Entry();
			containerEntry.setName(container.getName());
			root.getChildren().add(containerEntry);
			container.getProperties().getPublicAccess();
			BlobContainerClient blobcc = serviceClient.getBlobContainerClient(container.getName());

			PublicAccessType access = blobcc.getAccessPolicy().getBlobAccessType();
			containerEntry.blobcc = blobcc;

			containerEntry.setMetadata(blobcc.getProperties().getMetadata());

			containerEntry.accessPolicies = blobcc.getAccessPolicy().getIdentifiers();

			PagedIterable<BlobItem> blobList = blobcc.listBlobs();

			for (BlobItem blobitem : blobList) {
				Entry blobEntry = new Entry();
				blobEntry.setName(blobitem.getName());
				blobEntry.setBlobItem(blobitem);
				blobEntry.lastModified = blobitem.getProperties().getLastModified().toOffsetTime();
				blobEntry.accessTier = blobitem.getProperties().getAccessTier().toString();
				blobEntry.blobTier = blobitem.getProperties().getBlobType().toString();

				containerEntry.getChildren().add(blobEntry);
				blobEntry.setMetadata(blobitem.getMetadata());
				blobEntry.blobClient = blobcc.getBlobClient(blobitem.getName());

			}

		}

		treeGrid.setItems(root.getChildren(), Entry::getChildren);

	}

	@WebServlet(urlPatterns = "/*", name = "MyUIServlet", asyncSupported = true)
	@VaadinServletConfiguration(ui = MyUI.class, productionMode = false)
	public static class MyUIServlet extends VaadinServlet {
	}

	class Entry {
		public BlobContainerClient blobcc;

		public List<BlobSignedIdentifier> accessPolicies;

		public String blobTier;

		public String accessTier;

		public OffsetTime lastModified;

		public BlobClient blobClient;

		String name = "";

		String level = "";

		String policy = "";

		String accessLevel = "";

		Map<String, String> metadata;

		public void setAccessLevel(String accessLevel) {
			this.accessLevel = accessLevel;
		}

		public void setMetadata(Map<String, String> metadata) {
			this.metadata = metadata;

		}

		public String getAccessLevel() {
			return this.accessLevel;
		}

		public BlobItem getBlobItem() {
			return mBlobItem;
		}

		public void setBlobItem(BlobItem mBlobItem) {
			this.mBlobItem = mBlobItem;
		}

		BlobItem mBlobItem;

		List<Entry> children = new ArrayList<>();

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getLevel() {
			return level;
		}

		public void setLevel(String level) {
			this.level = level;
		}

		public String getPolicy() {
			return policy;
		}

		public void setPolicy(String policy) {
			this.policy = policy;
		}

		public List<Entry> getChildren() {
			return children;
		}

		public void setChildren(List<Entry> children) {
			this.children = children;
		}
	}
}
