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
 */
@Theme("mytheme")
public class MyUI extends UI {

    final static String STORAGE_ACCOUNT_NAME = "ibuoeqpzhmwnawebapp";
    // TODO: SECURITY key vault
    final static String STORAGE_ACCOUNT_KEY = "Q1hJKW8B+ulHMnwOpws7w2PlWIHQpiLH10+2SyixgA00PsSSzsSLisvzT5w3GIXeKkMfyDNHaKxZ1fJjCfYg6w==";
    // From Storage account - access keys
    final static String CONNECTION_KEY = "DefaultEndpointsProtocol=https"
            + ";AccountName=" + STORAGE_ACCOUNT_NAME
            + ";AccountKey=" + STORAGE_ACCOUNT_KEY
            + ";EndpointSuffix=core.windows.net";

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
        treeGrid.addSelectionListener(this::select);

        bsi.addColumn(BlobSignedIdentifier::getId).setCaption("Id");

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

            if (entry.getBlobContainerClient() != null) {
                BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder()
                        .containerClient(entry.getBlobContainerClient())
                        .buildClient();
                blobLeaseClient.acquireLease(60);
                Notification.show("Lease set!");
            } else if (entry.blobClient != null) {
                BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(entry.blobClient)
                        .buildClient();
                blobLeaseClient.acquireLease(60);
                Notification.show("Lease set!");
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
            Notification.show("No selection");
            return;
        }
        Entry entry = set.iterator().next();
        if (entry.blobClient == null) {
            Notification.show("No File selected.");
            return;
        }

        String cname = entry.blobClient.getContainerName();

        BlobSasPermission blobPermission = new BlobSasPermission().setReadPermission(true);

        BlobServiceSasSignatureValues builder = new BlobServiceSasSignatureValues().setProtocol(SasProtocol.HTTPS_ONLY)
                .setExpiryTime(OffsetDateTime.now().plusDays(2)).setContainerName(cname)
                .setBlobName(entry.blobClient.getBlobName());

        if (!bsi.getSelectedItems().isEmpty()) {

            BlobSignedIdentifier identifier = bsi.getSelectedItems().iterator().next();
            System.out.println("perm=" + identifier.getId());
            builder.setIdentifier(identifier.getId());
        } else {
            builder.setPermissions(blobPermission);
        }

        System.out.println("" + entry.blobClient.getBlobUrl());

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(STORAGE_ACCOUNT_NAME,
                STORAGE_ACCOUNT_KEY);
        BlobServiceSasQueryParameters sasQueryParameters = builder.generateSasQueryParameters(credential);
        System.out.println(entry.blobClient.getBlobUrl() + "?" + sasQueryParameters.encode());

        url.setValue(entry.blobClient.getBlobUrl() + "?" + sasQueryParameters.encode());
    }

    private void select(SelectionEvent<Entry> e) {
        if (e.getFirstSelectedItem().isPresent()) {
            Entry entry = e.getFirstSelectedItem().get();
            if (entry.metadata != null) {
                StringBuilder value = new StringBuilder();
                for (String key : entry.metadata.keySet()) {
                    value.append(key).append("=").append(entry.metadata.get(key)).append("\n");
                }
                metaData.setValue(value.toString());
            } else {
                metaData.setValue("-" + e.getFirstSelectedItem().get().name);
            }
            if (entry.accessPolicies != null) {
                bsi.setItems(entry.accessPolicies);
            }
        }
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

            containerEntry.setBlobContainerClient(blobcc);

            containerEntry.setMetadata(blobcc.getProperties().getMetadata());

            containerEntry.accessPolicies = blobcc.getAccessPolicy().getIdentifiers();

            PagedIterable<BlobItem> blobList = blobcc.listBlobs();

            for (BlobItem blobitem : blobList) {
                Entry blobEntry = new Entry().setName(blobitem.getName()).setBlobItem(blobitem)
                        .setLastModified(blobitem.getProperties().getLastModified().toOffsetTime())
                        .setAccessTier(blobitem.getProperties().getAccessTier().toString())
                        .setBlobTier(blobitem.getProperties().getBlobType().toString());

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

    static class Entry {
        BlobContainerClient blobContainerClient;
        List<BlobSignedIdentifier> accessPolicies;
        String blobTier;
        String accessTier;
        OffsetTime lastModified;
        BlobClient blobClient;
        String name = "";
        String level = "";
        String policy = "";
        String accessLevel = "";
        Map<String, String> metadata;
        BlobItem blobItem;
        List<Entry> children = new ArrayList<>();

        public BlobContainerClient getBlobContainerClient() {
            return blobContainerClient;
        }

        public Entry setBlobContainerClient(BlobContainerClient blobContainerClient) {
            this.blobContainerClient = blobContainerClient;
            return this;
        }

        public List<BlobSignedIdentifier> getAccessPolicies() {
            return accessPolicies;
        }

        public Entry setAccessPolicies(List<BlobSignedIdentifier> accessPolicies) {
            this.accessPolicies = accessPolicies;
            return this;
        }

        public String getBlobTier() {
            return blobTier;
        }

        public Entry setBlobTier(String blobTier) {
            this.blobTier = blobTier;
            return this;
        }

        public String getAccessTier() {
            return accessTier;
        }

        public Entry setAccessTier(String accessTier) {
            this.accessTier = accessTier;
            return this;
        }

        public OffsetTime getLastModified() {
            return lastModified;
        }

        public Entry setLastModified(OffsetTime lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public BlobClient getBlobClient() {
            return blobClient;
        }

        public Entry setBlobClient(BlobClient blobClient) {
            this.blobClient = blobClient;
            return this;
        }

        public String getName() {
            return name;
        }

        public Entry setName(String name) {
            this.name = name;
            return this;
        }

        public String getLevel() {
            return level;
        }

        public Entry setLevel(String level) {
            this.level = level;
            return this;
        }

        public String getPolicy() {
            return policy;
        }

        public Entry setPolicy(String policy) {
            this.policy = policy;
            return this;
        }

        public String getAccessLevel() {
            return accessLevel;
        }

        public Entry setAccessLevel(String accessLevel) {
            this.accessLevel = accessLevel;
            return this;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public Entry setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public BlobItem getBlobItem() {
            return blobItem;
        }

        public Entry setBlobItem(BlobItem blobItem) {
            this.blobItem = blobItem;
            return this;
        }

        public List<Entry> getChildren() {
            return children;
        }

        public Entry setChildren(List<Entry> children) {
            this.children = children;
            return this;
        }
    }
}
