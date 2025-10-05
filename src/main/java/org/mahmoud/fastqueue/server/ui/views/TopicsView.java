package org.mahmoud.fastqueue.server.ui.views;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H1;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import org.mahmoud.fastqueue.server.ui.layout.MainLayout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Topics management view for creating, listing, and managing topics
 */
@Route(value = "topics", layout = MainLayout.class)
@PageTitle("Topics | FastQueue2")
public class TopicsView extends VerticalLayout {
    
    private Grid<TopicInfo> topicsGrid;
    private List<TopicInfo> topics;
    
    public TopicsView() {
        addClassNames(LumoUtility.Padding.LARGE);
        
        H1 title = new H1("Topic Management");
        title.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.Margin.Bottom.LARGE);
        
        // Create topic button
        Button createButton = new Button("Create Topic", VaadinIcon.PLUS.create());
        createButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        createButton.addClickListener(e -> showCreateTopicDialog());
        
        HorizontalLayout headerLayout = new HorizontalLayout(title, createButton);
        headerLayout.setJustifyContentMode(JustifyContentMode.BETWEEN);
        headerLayout.setAlignItems(Alignment.CENTER);
        headerLayout.setWidthFull();
        
        // Initialize topics data
        initializeTopics();
        
        // Create topics grid
        createTopicsGrid();
        
        add(headerLayout, topicsGrid);
    }
    
    private void initializeTopics() {
        topics = new ArrayList<>();
        // Mock data
        topics.add(new TopicInfo("orders", 1250, LocalDateTime.now().minusDays(2)));
        topics.add(new TopicInfo("notifications", 3420, LocalDateTime.now().minusDays(1)));
        topics.add(new TopicInfo("events", 890, LocalDateTime.now().minusHours(5)));
        topics.add(new TopicInfo("logs", 15600, LocalDateTime.now().minusDays(7)));
        topics.add(new TopicInfo("metrics", 2100, LocalDateTime.now().minusDays(3)));
    }
    
    private void createTopicsGrid() {
        topicsGrid = new Grid<>(TopicInfo.class, false);
        topicsGrid.addColumn(TopicInfo::getName).setHeader("Topic Name").setSortable(true);
        topicsGrid.addColumn(TopicInfo::getMessageCount).setHeader("Messages").setSortable(true);
        topicsGrid.addColumn(TopicInfo::getCreatedAt).setHeader("Created").setSortable(true);
        
        topicsGrid.addComponentColumn(topic -> {
            Button deleteButton = new Button(VaadinIcon.TRASH.create());
            deleteButton.addThemeVariants(ButtonVariant.LUMO_ERROR, ButtonVariant.LUMO_SMALL);
            deleteButton.addClickListener(e -> deleteTopic(topic));
            return deleteButton;
        }).setHeader("Actions").setWidth("120px");
        
        topicsGrid.setItems(topics);
        topicsGrid.setWidthFull();
    }
    
    private void showCreateTopicDialog() {
        Dialog dialog = new Dialog();
        dialog.setHeaderTitle("Create New Topic");
        
        FormLayout formLayout = new FormLayout();
        TextField nameField = new TextField("Topic Name");
        nameField.setPlaceholder("Enter topic name");
        nameField.setRequired(true);
        
        formLayout.add(nameField);
        
        Button saveButton = new Button("Create", VaadinIcon.CHECK.create());
        saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        saveButton.addClickListener(e -> {
            if (!nameField.getValue().trim().isEmpty()) {
                createTopic(nameField.getValue().trim());
                dialog.close();
            }
        });
        
        Button cancelButton = new Button("Cancel");
        cancelButton.addClickListener(e -> dialog.close());
        
        HorizontalLayout buttonLayout = new HorizontalLayout(saveButton, cancelButton);
        buttonLayout.setJustifyContentMode(JustifyContentMode.END);
        
        VerticalLayout dialogContent = new VerticalLayout(formLayout, buttonLayout);
        dialogContent.setPadding(false);
        
        dialog.add(dialogContent);
        dialog.open();
    }
    
    private void createTopic(String name) {
        // Check if topic already exists
        boolean exists = topics.stream().anyMatch(topic -> topic.getName().equals(name));
        if (exists) {
            // Show error notification
            return;
        }
        
        TopicInfo newTopic = new TopicInfo(name, 0, LocalDateTime.now());
        topics.add(newTopic);
        topicsGrid.setItems(topics);
    }
    
    private void deleteTopic(TopicInfo topic) {
        topics.remove(topic);
        topicsGrid.setItems(topics);
    }
    
    // Inner class for topic information
    public static class TopicInfo {
        private String name;
        private int messageCount;
        private LocalDateTime createdAt;
        
        public TopicInfo(String name, int messageCount, LocalDateTime createdAt) {
            this.name = name;
            this.messageCount = messageCount;
            this.createdAt = createdAt;
        }
        
        public String getName() { return name; }
        public int getMessageCount() { return messageCount; }
        public LocalDateTime getCreatedAt() { return createdAt; }
    }
}
