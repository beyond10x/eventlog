import docsSystemPlugin, {ecosystemFooterGroup, ecosystemNavbarItems} from '@beyond10x/docs-system/docusaurus';

export default {
  title: 'Eventlog',
  tagline: 'Durable history, rebuildable state',
  url: 'https://beyond10x.github.io',
  baseUrl: '/eventlog/',
  organizationName: 'beyond10x',
  projectName: 'eventlog',
  trailingSlash: false,
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  markdown: {format: 'md', hooks: {onBrokenMarkdownLinks: 'throw'}},
  presets: [['classic', {
    docs: {path: 'docs', routeBasePath: '/', sidebarPath: './sidebars.js'},
    blog: false,
  }]],
  plugins: [docsSystemPlugin],
  themeConfig: {
    colorMode: {defaultMode: 'dark', respectPrefersColorScheme: true},
    navbar: {title: 'Eventlog', items: [
      ...ecosystemNavbarItems(),
      {to: '/quickstart', label: 'Quickstart', position: 'left'},
      {to: '/providers', label: 'Providers', position: 'left'},
      {to: '/operations', label: 'Operate', position: 'left'},
      {href: 'https://github.com/beyond10x/eventlog', label: 'Source', position: 'right'},
    ]},
    footer: {style: 'dark', links: [ecosystemFooterGroup()]},
    prism: {additionalLanguages: ['rust', 'toml', 'bash']},
  },
};
