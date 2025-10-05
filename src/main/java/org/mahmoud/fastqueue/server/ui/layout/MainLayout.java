package org.mahmoud.fastqueue.server.ui.layout;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.applayout.AppLayout;
import com.vaadin.flow.component.applayout.DrawerToggle;
import com.vaadin.flow.component.html.H1;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.sidenav.SideNav;
import com.vaadin.flow.component.sidenav.SideNavItem;
import com.vaadin.flow.router.RouterLayout;
import com.vaadin.flow.theme.lumo.LumoUtility;
import org.mahmoud.fastqueue.server.ui.views.DashboardView;
import org.mahmoud.fastqueue.server.ui.views.TopicsView;
import org.mahmoud.fastqueue.server.ui.views.MessagesView;
import org.mahmoud.fastqueue.server.ui.views.MetricsView;
import org.mahmoud.fastqueue.server.ui.views.HealthView;

/**
 * Main layout for FastQueue2 Web UI
 * Provides navigation and consistent layout across all views
 */
public class MainLayout extends AppLayout implements RouterLayout {
    
    public MainLayout() {
        createHeader();
        createDrawer();
    }
    
    private void createHeader() {
        H1 logo = new H1("FastQueue2");
        logo.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.Margin.NONE);
        
        HorizontalLayout header = new HorizontalLayout(new DrawerToggle(), logo);
        header.setDefaultVerticalComponentAlignment(FlexComponent.Alignment.CENTER);
        header.expand(logo);
        header.setWidthFull();
        header.addClassNames(LumoUtility.Padding.Vertical.NONE, LumoUtility.Padding.Horizontal.MEDIUM);
        
        addToNavbar(header);
    }
    
    private void createDrawer() {
        H2 appName = new H2("FastQueue2");
        appName.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.Margin.NONE);
        
        SideNav nav = new SideNav();
        
        nav.addItem(new SideNavItem("Dashboard", DashboardView.class, VaadinIcon.DASHBOARD.create()));
        nav.addItem(new SideNavItem("Topics", TopicsView.class, VaadinIcon.LIST.create()));
        nav.addItem(new SideNavItem("Messages", MessagesView.class, VaadinIcon.ENVELOPE.create()));
        nav.addItem(new SideNavItem("Metrics", MetricsView.class, VaadinIcon.CHART.create()));
        nav.addItem(new SideNavItem("Health", HealthView.class, VaadinIcon.HEART.create()));
        
        VerticalLayout drawerContent = new VerticalLayout(appName, nav);
        drawerContent.setPadding(false);
        drawerContent.setSpacing(false);
        drawerContent.setAlignItems(FlexComponent.Alignment.STRETCH);
        drawerContent.setSizeFull();
        
        addToDrawer(drawerContent);
    }
}
