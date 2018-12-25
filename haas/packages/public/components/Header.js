// An example header
// To make anything ssr-only, just replace <Link> with <a>
// and ofcourse remove the inner <a>

import { Component, Fragment } from 'react';
import PropTypes from 'prop-types';

import Link from 'next/link';
import Router, { withRouter } from 'next/router';

// import MenuItem from '@material-ui/core/MenuItem';
// import Menu from '@material-ui/core/Menu';

import classNames from 'classnames';
import { view } from 'react-easy-state';

const LoginLink = (props, context) => {
  return (
    <React.Fragment>
      <a
        color="default"
        label="Log In"
        className="link-button"
        onClick={props.onLogin}
      >
        Login
      </a>
    </React.Fragment>
  );
};

class Header extends Component {
  constructor(props) {
    super(props);

    this.state = {
      anchorEl: null,
      openLogoutSnackbar: false
    };
  }

  // ES7-style arrow class method; to provide object-referencing "this"
  handleChange = event => {
    this.setState({ auth: event.target.checked });
  };

  handleMenu = event => {
    this.setState({ anchorEl: event.currentTarget });
  };

  handleClose = () => {
    this.setState({ anchorEl: null });
  };

  // Render does not get (props, state) passed to it
  render() {
    const { classes, children, className, onLogin, onLogout } = this.props;
    const { pathname } = this.props.router;

    const { anchorEl } = this.state;
    const open = Boolean(anchorEl);

    // const { palette } = this.props.pageContext.theme;

    // const isDark = palette.type === 'dark';
    const isDark = 'false';
    return (
      <span id="Header">
        <div id="appBar">
          <Link href="/" passHref>
            <a
              href="/"
              label="Home"
              id="home"
              className={pathname === '/' ? 'is-active' : null}
            >
              H
            </a>
          </Link>
          <Link href="/submit" passHref prefetch>
            <a
              aria-label="Suubmit"
              className={classNames('link-button', {
                'is-active': pathname === '/submit'
              })}
            >
              Submit
            </a>
          </Link>
          <Link href="/batch" passHref prefetch>
            <a
              aria-label="Batch"
              className={classNames('link-button', {
                'is-active': pathname === '/batch'
              })}
            >
              Batch
            </a>
          </Link>
          <Link href="/scorecard" passHref prefetch>
            <a
              aria-label="Scorecard"
              className={classNames('link-button', {
                'is-active': pathname === '/scorecard'
              })}
            >
              Scorecard
            </a>
          </Link>

          <Link href="/jobs" passHref prefetch>
            <a
              aria-label="Jobs"
              className={classNames('link-button', {
                'is-active': pathname === '/jobs'
              })}
            >
              Jobs
            </a>
          </Link>

          <Link href="/tutorial" passHref>
            <a
              aria-label="Tutorial"
              className={classNames('link-button', {
                'is-active': pathname === '/tutorial'
              })}
            >
              Tutorial
            </a>
          </Link>

          <span style={{ marginLeft: 'auto' }}>
            {this.props.auth.state.user ? (
              <span
                aria-owns={open ? 'menu-appbar' : null}
                aria-haspopup="true"
                onClick={this.handleMenu}
                color="inherit"
                style={{ marginTop: -5 }}
                className="link-button"
              >
                <i className="material-icons">account_circle</i>
              </span>
            ) : (
              <LoginLink onLogin={onLogin} />
            )}
          </span>
        </div>
      </span>
    );
  }
}

export { LoginLink };
export default withRouter(view(Header));
